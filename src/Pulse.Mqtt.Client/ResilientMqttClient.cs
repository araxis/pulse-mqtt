using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Serialization;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Client;

/// <summary>
/// The always-resilient MQTT client: it connects in the background, survives broker drops with the
/// configured reconnect strategy, re-subscribes before any traffic flows, queues publishes while
/// offline and flushes them in order, and stops cleanly on terminal failures instead of retrying
/// forever. <see cref="Messages"/> and <see cref="WatchState"/> survive reconnects.
/// </summary>
public sealed class ResilientMqttClient : IAsyncDisposable
{
    // A redirect chain that pauses this long is considered over, so the hop budget renews.
    // Misconfigured ping-pong loops cycle in seconds; legitimate rebalances are minutes apart.
    private static readonly TimeSpan RedirectChainQuietPeriod = TimeSpan.FromSeconds(60);

    // Mutable only for redirect following: the supervisor loop is the sole reader and writer.
    private IMqttTransportFactory _transportFactory;
    private int _redirectHops;
    private long _lastRedirectTimestamp;
    private readonly ResilientMqttClientOptions _options;
    private readonly TimeProvider _time;
    private readonly IReconnectStrategy _strategy;
    private readonly IConnectionLifecycle _lifecycle;
    private readonly ISessionStore _sessionStore;
    private readonly IMessageStore _messageStore;
    private readonly IReconnectDecision _decision;
    private readonly MqttInFlightSession? _inFlightSession;
    private readonly Channel<MqttPublishPacket> _messages;
    private readonly List<Channel<ConnectionStateChanged>> _watchers = [];
    private readonly Dictionary<string, MqttTopicFilter> _subscriptions = [];

    // Subscribe/unsubscribe changes made while offline (or when a live call lost the connection). The
    // lifecycle replays the full set only on a fresh session; on a resumed one (SessionPresent = true)
    // it skips, so this delta is what the broker would otherwise never hear about. Guarded by _subscriptionGate.
    private readonly Dictionary<string, MqttTopicFilter> _pendingSubscribe = [];
    private readonly HashSet<string> _pendingUnsubscribe = [];

    private readonly object _stateGate = new();
    private readonly object _subscriptionGate = new();

    // Serializes offline-queue flushes: the connection-up flush and the post-enqueue nudge in
    // PublishCoreAsync must not interleave, or two drains could double-send the same head.
    private readonly SemaphoreSlim _flushGate = new(1, 1);

    private readonly Lazy<MqttRouter> _router;
    private readonly string _clientId;
    private readonly PulseMqttDiagnostics.IOfflineQueueProbe _offlineProbe;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<MqttPublishPacket>> _pendingRequests = new();
    private readonly ConcurrentDictionary<string, StreamSink> _pendingStreams = new();
    private readonly object _rpcGate = new();
    private Task? _rpcRoute;
    private CancellationTokenSource? _lifetime;
    private Task? _supervisor;
    private volatile RawMqttClient? _raw;
    private int _attempt;
    private DateTimeOffset _stateChangedAt;
    private MqttReasonCode? _lastReason;
    private string? _lastReasonString;
    private string? _lastServerReference;
    private Exception? _lastError;
    private MqttBrokerCapabilitiesSnapshot? _brokerCapabilities;
    private volatile bool _disposed;

    /// <summary>Creates a resilient client that connects through <paramref name="transportFactory"/>.</summary>
    public ResilientMqttClient(
        IMqttTransportFactory transportFactory,
        ResilientMqttClientOptions options,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(transportFactory);
        ArgumentNullException.ThrowIfNull(options);

        _transportFactory = transportFactory;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _stateChangedAt = _time.GetUtcNow();

        _sessionStore = options.SessionStore ?? new InMemorySessionStore();
        _messageStore = options.MessageStore ?? new InMemoryMessageStore(options.OfflineQueue);
        _decision = options.ReconnectDecision ?? new DefaultReconnectDecision();
        _strategy = options.ReconnectStrategy ?? new BackoffReconnectStrategy(options.Backoff, _decision);
        _lifecycle = options.Lifecycle ?? new DefaultConnectionLifecycle(_sessionStore);

        // In-flight QoS redelivery is only meaningful for a persistent session — a client that
        // asks for one (CleanStart = false). For clean-start clients the tracking is skipped
        // entirely, so the hot publish path keeps its zero-allocation cost.
        if (!options.Connect.CleanStart)
        {
            _inFlightSession = new MqttInFlightSession(
                (state, token) => _sessionStore.SaveInFlightAsync(state, token));
        }

        _messages = Channel.CreateBounded<MqttPublishPacket>(new BoundedChannelOptions(options.Raw.InboundMessageCapacity)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        _clientId = options.Connect.ClientId;
        _offlineProbe = new OfflineQueueProbe(_clientId, _messageStore);
        PulseMqttDiagnostics.RegisterOfflineQueue(_offlineProbe);
        _router = new Lazy<MqttRouter>(
            () =>
            {
                var router = new MqttRouter(_messages.Reader);
                if (options.Logger is { } logger)
                {
                    router.HandlerFaulted += (template, error) => PulseMqttLog.RouteHandlerFaulted(logger, template, error);
                }

                router.Start();
                return router;
            },
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>
    /// Backstop for a client abandoned without <see cref="DisposeAsync"/>: removing the probe from
    /// the process-global diagnostics registry lets its message store (and any queued payloads) be
    /// collected. <see cref="DisposeAsync"/> suppresses this finalizer on the normal path.
    /// </summary>
    ~ResilientMqttClient() => PulseMqttDiagnostics.UnregisterOfflineQueue(_offlineProbe);

    /// <summary>The current connection state.</summary>
    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    /// <summary>Raised on every state transition.</summary>
    public event Action<ConnectionStateChanged>? StateChanged;

    /// <summary>
    /// Received application messages across all sessions, in arrival order. Completes only when
    /// the client is disposed. Use either this reader or <see cref="Router"/> — once the router is
    /// created it owns consumption of the stream.
    /// </summary>
    public ChannelReader<MqttPublishPacket> Messages => _messages.Reader;

    /// <summary>
    /// The topic router over <see cref="Messages"/>. Created (and started) on first access; from
    /// then on the router owns the message stream — do not also read <see cref="Messages"/> directly.
    /// </summary>
    public MqttRouter Router => _router.Value;

    internal MqttProtocolVersion ConfiguredProtocolVersion => _options.Connect.ProtocolVersion;

    /// <summary>Returns a synchronous point-in-time diagnostics snapshot for this client.</summary>
    public MqttClientDiagnosticsSnapshot GetDiagnosticsSnapshot()
    {
        var (offlineQueueDepth, offlineQueueDroppedCount) = ReadOfflineQueueCounters(_messageStore);

        int subscriptionCount;
        int pendingSubscribeCount;
        int pendingUnsubscribeCount;
        lock (_subscriptionGate)
        {
            subscriptionCount = _subscriptions.Count;
            pendingSubscribeCount = _pendingSubscribe.Count;
            pendingUnsubscribeCount = _pendingUnsubscribe.Count;
        }

        lock (_stateGate)
        {
            return new MqttClientDiagnosticsSnapshot(
                _clientId,
                State,
                _attempt,
                _supervisor is { IsCompleted: false },
                _stateChangedAt,
                _lastReason,
                _lastReasonString,
                _lastServerReference,
                _lastError,
                offlineQueueDepth,
                offlineQueueDroppedCount,
                subscriptionCount,
                pendingSubscribeCount,
                pendingUnsubscribeCount)
            {
                BrokerCapabilities = State == ConnectionState.Connected ? _brokerCapabilities : null,
            };
        }
    }

    /// <summary>Returns the broker capabilities negotiated for the current connection, if connected.</summary>
    public MqttBrokerCapabilitiesSnapshot? GetBrokerCapabilitiesSnapshot()
    {
        lock (_stateGate)
        {
            return State == ConnectionState.Connected ? _brokerCapabilities : null;
        }
    }

    /// <summary>Returns the current support state for a library-known MQTT protocol feature.</summary>
    public MqttBrokerFeatureSupport GetProtocolFeatureSupport(MqttProtocolFeature feature)
    {
        var protocolVersion = _options.Connect.ProtocolVersion;
        if (!MqttProtocolFeatures.IsSupported(protocolVersion, feature))
        {
            return MqttBrokerFeatureSupport.NotSupported;
        }

        if (!IsBrokerNegotiatedFeature(feature))
        {
            return MqttBrokerFeatureSupport.Supported;
        }

        lock (_stateGate)
        {
            if (State == ConnectionState.Connected && _brokerCapabilities is { } capabilities)
            {
                return capabilities.GetFeatureSupport(feature);
            }
        }

        return MqttBrokerFeatureSupport.Unknown;
    }

    /// <summary>Returns <c>true</c> when a library-known MQTT protocol feature is available now.</summary>
    public bool CanUseProtocolFeature(MqttProtocolFeature feature) =>
        GetProtocolFeatureSupport(feature) == MqttBrokerFeatureSupport.Supported;

    /// <summary>Throws <see cref="NotSupportedException"/> when a library-known MQTT protocol feature is unavailable now.</summary>
    public void EnsureProtocolFeature(MqttProtocolFeature feature, string? operation = null)
    {
        var support = GetProtocolFeatureSupport(feature);
        if (support == MqttBrokerFeatureSupport.Supported)
        {
            return;
        }

        var operationText = string.IsNullOrWhiteSpace(operation)
            ? "This operation"
            : operation;

        var message = support == MqttBrokerFeatureSupport.Unknown
            ? $"{operationText} requires MQTT feature '{feature}', but broker support is unknown until the client is connected."
            : $"{operationText} requires MQTT feature '{feature}', but it is not supported by the current MQTT protocol or broker capabilities.";

        throw new NotSupportedException(message);
    }

    /// <summary>
    /// Connects this resilient client. The first connection attempt starts immediately, while
    /// reconnects continue in the background; watch <see cref="State"/> for progress.
    /// </summary>
    /// <exception cref="InvalidOperationException">The client is already running.</exception>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_stateGate)
        {
            if (_supervisor is { IsCompleted: false })
            {
                throw new InvalidOperationException("The client is already running.");
            }
        }

        var stored = await _sessionStore.LoadSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
        lock (_subscriptionGate)
        {
            _subscriptions.Clear();
            foreach (var filter in stored)
            {
                _subscriptions[filter.Topic] = filter;
            }
        }

        // Restore any in-flight QoS state a durable store persisted across a restart, so the
        // first connection can redeliver it.
        if (_inFlightSession is not null)
        {
            _inFlightSession.Restore(await _sessionStore.LoadInFlightAsync(cancellationToken).ConfigureAwait(false));
        }

        _attempt = 0;
        _lifetime?.Dispose();
        var lifetime = new CancellationTokenSource();
        _lifetime = lifetime;

        // Started inline: the first connect attempt reaches its first await (the socket connect)
        // on the calling task instead of waiting for a thread-pool slot.
        _supervisor = SuperviseAsync(lifetime.Token);
    }

    /// <summary>
    /// Starts the supervisor. Prefer <see cref="ConnectAsync"/> for application code; this alias
    /// remains for source compatibility with earlier releases.
    /// </summary>
    [Obsolete("Use ConnectAsync. StartAsync is retained only as a compatibility alias.")]
    public Task StartAsync(CancellationToken cancellationToken = default) => ConnectAsync(cancellationToken);

    /// <summary>
    /// Publishes through the live connection when available; otherwise queues (QoS &gt; 0, or QoS 0
    /// when configured) or drops QoS 0 — always explicitly, never silently.
    /// </summary>
    public async Task<PublishOutcome> PublishAsync(MqttPublishPacket packet, CancellationToken cancellationToken = default)
    {
        return await PublishAsync(packet, transform: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<PublishOutcome> PublishAsync(
        MqttPublishPacket packet,
        Func<MqttPublishPacket, ActivityContext?, MqttPublishPacket>? transform,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(packet);
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var activity = PulseMqttDiagnostics.ActivitySource.StartActivity("publish", ActivityKind.Producer);
        if (activity is not null)
        {
            activity.DisplayName = $"publish {packet.Topic}";
            activity.SetTag("messaging.system", "mqtt");
            activity.SetTag("messaging.destination.name", packet.Topic);
            activity.SetTag("messaging.operation.type", "send");
        }

        // Inject the active span (ours, or an ambient one the caller already started) so a consumer
        // can parent its receive span on this publish. Off unless the caller opts in.
        var currentContext = Activity.Current?.Context;
        if (_options.PropagateTraceContext &&
            CanUseProtocolFeature(MqttProtocolFeature.TraceContextUserProperties) &&
            currentContext is { } current)
        {
            packet = TraceContextPropagation.Inject(packet, current);
        }

        if (transform is not null)
        {
            packet = transform(packet, currentContext);
        }

        var start = _time.GetTimestamp();
        var outcome = await PublishCoreAsync(packet, cancellationToken).ConfigureAwait(false);
        var elapsed = _time.GetElapsedTime(start).TotalSeconds;

        // Interned name, computed once: the enum's own ToString() would allocate a string on every
        // publish — measurable on the hot path even with no telemetry listener attached.
        var disposition = DispositionName(outcome.Disposition);
        var clientIdTag = new KeyValuePair<string, object?>("client.id", _clientId);
        var dispositionTag = new KeyValuePair<string, object?>("disposition", disposition);
        activity?.SetTag("pulse.mqtt.disposition", disposition);
        PulseMqttDiagnostics.MessagesPublished.Add(1, clientIdTag, dispositionTag);
        PulseMqttDiagnostics.PublishDuration.Record(elapsed, clientIdTag, dispositionTag);
        return outcome;
    }

    private static string DispositionName(PublishDisposition disposition) => disposition switch
    {
        PublishDisposition.Delivered => "Delivered",
        PublishDisposition.Queued => "Queued",
        PublishDisposition.DroppedOffline => "DroppedOffline",
        PublishDisposition.InFlight => "InFlight",
        _ => disposition.ToString(),
    };

    private async Task<PublishOutcome> PublishCoreAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
    {
        if (_raw is { } raw)
        {
            try
            {
                var reason = await raw.PublishAsync(packet, cancellationToken).ConfigureAwait(false);
                return new PublishOutcome(PublishDisposition.Delivered, reason);
            }
            catch (MqttPacketTooLargeException)
            {
                // Queueing would retry a permanently-doomed packet forever; the caller decides.
                throw;
            }
            catch (MqttPublishInFlightException)
            {
                // The connection died mid-exchange while a persistent session was tracking this
                // publish: the session holds it and redelivers it with DUP on resume. Queueing it
                // too would double-send, so it returns as in-flight, not queued.
                return new PublishOutcome(PublishDisposition.InFlight);
            }
            catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
            {
                // The connection died underneath us; fall through to the offline path.
            }
        }

        if (packet.QualityOfService == MqttQualityOfService.AtMostOnce && !_options.OfflineQueue.IncludeQos0)
        {
            return new PublishOutcome(PublishDisposition.DroppedOffline);
        }

        await _messageStore.EnqueueAsync(packet, _time.GetUtcNow(), cancellationToken).ConfigureAwait(false);

        // The connection may have come up while this publish was between the _raw check and the
        // enqueue, with the connection-up flush already past its final queue scan. Nothing else
        // ever flushes while Connected, so without this re-check the message would sit in the
        // queue until the NEXT reconnect — or forever (the chaos-suite one-message loss).
        if (_raw is { } liveNow)
        {
            try
            {
                await FlushQueuedGatedAsync(liveNow, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The connection died again while nudging the queue. The message the caller asked
                // to publish is already enqueued (the outcome below is Queued), so a connection-death
                // exception from this best-effort flush must not escape — and the type is the
                // transport's choice (MqttException, IOException, SocketException, QuicException, ...),
                // so it cannot be enumerated. The next connection-up flush drains the queue.
                // Caller cancellation still propagates.
            }
        }

        return new PublishOutcome(PublishDisposition.Queued);
    }

    private async Task FlushQueuedGatedAsync(RawMqttClient raw, CancellationToken cancellationToken)
    {
        await _flushGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await FlushQueuedAsync(raw, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _flushGate.Release();
        }
    }

    /// <summary>
    /// Adds subscriptions to the durable set and applies them on the live connection when one
    /// exists. Offline, they apply on the next connection-up; the result list is then empty.
    /// </summary>
    public async Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        IReadOnlyList<MqttTopicFilter> topicFilters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_subscriptionGate)
        {
            foreach (var filter in topicFilters)
            {
                _subscriptions[filter.Topic] = filter;
            }
        }

        await _sessionStore.UpsertSubscriptionsAsync(topicFilters, cancellationToken).ConfigureAwait(false);

        if (_raw is { } raw)
        {
            try
            {
                var granted = await raw.SubscribeAsync(topicFilters, cancellationToken).ConfigureAwait(false);
                lock (_subscriptionGate)
                {
                    foreach (var filter in topicFilters)
                    {
                        _pendingSubscribe.Remove(filter.Topic);
                        _pendingUnsubscribe.Remove(filter.Topic);
                    }
                }

                return granted;
            }
            catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
            {
                // The connection died mid-subscribe; fall through to record it as pending so the next
                // connection-up applies it even when the broker resumes the session.
            }
        }

        // Offline (or the live subscribe lost the connection): the broker has not heard this change.
        // Record it so connection-up replays it even on a resumed session, where the lifecycle skips.
        lock (_subscriptionGate)
        {
            foreach (var filter in topicFilters)
            {
                _pendingSubscribe[filter.Topic] = filter;
                _pendingUnsubscribe.Remove(filter.Topic);
            }
        }

        return [];
    }

    /// <summary>Removes subscriptions from the durable set and from the live connection when one exists.</summary>
    public async Task<IReadOnlyList<MqttReasonCode>> UnsubscribeAsync(
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_subscriptionGate)
        {
            foreach (var topic in topicFilters)
            {
                _subscriptions.Remove(topic);
            }
        }

        await _sessionStore.RemoveSubscriptionsAsync(topicFilters, cancellationToken).ConfigureAwait(false);

        if (_raw is { } raw)
        {
            try
            {
                var result = await raw.UnsubscribeAsync(topicFilters, cancellationToken).ConfigureAwait(false);
                lock (_subscriptionGate)
                {
                    foreach (var topic in topicFilters)
                    {
                        _pendingUnsubscribe.Remove(topic);
                        _pendingSubscribe.Remove(topic);
                    }
                }

                return result;
            }
            catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
            {
                // The connection died mid-unsubscribe; fall through to record it as pending.
            }
        }

        // Offline (or the live unsubscribe lost the connection): a resumed session still holds this
        // filter, so record the removal for connection-up to apply even when SessionPresent is true.
        lock (_subscriptionGate)
        {
            foreach (var topic in topicFilters)
            {
                _pendingUnsubscribe.Add(topic);
                _pendingSubscribe.Remove(topic);
            }
        }

        return [];
    }

    /// <summary>
    /// Registers a local handler for a route template (for example <c>sensors/{deviceId}/temp</c>).
    /// Subscribe the matching broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public IDisposable RegisterRoute(
        MqttRouteTemplate template,
        MqttRouteHandler handler) =>
        RegisterRoute(template, handler, options: null);

    /// <summary>
    /// Registers a local handler for a route template (for example <c>sensors/{deviceId}/temp</c>).
    /// Subscribe the matching broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public IDisposable RegisterRoute(
        MqttRouteTemplate template,
        MqttRouteHandler handler,
        MqttRouteOptions? options) =>
        Router.RegisterRoute(template, handler, options);

    /// <summary>
    /// Registers a local handler for a route template given as text. Subscribe the matching broker
    /// filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public IDisposable RegisterRoute(
        string template,
        MqttRouteHandler handler) =>
        RegisterRoute(MqttRouteTemplate.Parse(template), handler, options: null);

    /// <summary>
    /// Registers a local handler for a route template given as text. Subscribe the matching broker
    /// filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public IDisposable RegisterRoute(
        string template,
        MqttRouteHandler handler,
        MqttRouteOptions? options) =>
        RegisterRoute(MqttRouteTemplate.Parse(template), handler, options);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template. Subscribe the matching
    /// broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttRouteStream OpenRouteStream(MqttRouteTemplate template) =>
        OpenRouteStream(template, options: null);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template. Subscribe the matching
    /// broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttRouteStream OpenRouteStream(MqttRouteTemplate template, MqttRouteOptions? options) =>
        Router.OpenRouteStream(template, options);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template given as text. Subscribe
    /// the matching broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttRouteStream OpenRouteStream(string template) =>
        OpenRouteStream(MqttRouteTemplate.Parse(template), options: null);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template given as text. Subscribe
    /// the matching broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttRouteStream OpenRouteStream(string template, MqttRouteOptions? options) =>
        OpenRouteStream(MqttRouteTemplate.Parse(template), options);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template where the consumer
    /// explicitly acknowledges or rejects each matching inbound message. Subscribe the matching
    /// broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(MqttRouteTemplate template) =>
        OpenAcknowledgedRouteStream(template, options: null);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template where the consumer
    /// explicitly acknowledges or rejects each matching inbound message. Subscribe the matching
    /// broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(MqttRouteTemplate template, MqttRouteOptions? options) =>
        Router.OpenAcknowledgedRouteStream(template, options);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template given as text where the
    /// consumer explicitly acknowledges or rejects each matching inbound message. Subscribe the
    /// matching broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(string template) =>
        OpenAcknowledgedRouteStream(MqttRouteTemplate.Parse(template), options: null);

    /// <summary>
    /// Opens an <c>await foreach</c>-able stream for a local route template given as text where the
    /// consumer explicitly acknowledges or rejects each matching inbound message. Subscribe the
    /// matching broker filter separately with <see cref="SubscribeAsync"/>.
    /// </summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(string template, MqttRouteOptions? options) =>
        OpenAcknowledgedRouteStream(MqttRouteTemplate.Parse(template), options);

    // Applies subscribe/unsubscribe changes made while offline. On a fresh session the lifecycle has
    // already replayed the full stored set (which contains the offline additions, and excludes the
    // offline removals), so the broker already matches and the pending delta is simply dropped. On a
    // resumed session the lifecycle did nothing, so the delta is exactly what the broker is missing.
    private async Task ReconcileOfflineSubscriptionsAsync(RawMqttClient raw, bool sessionPresent, CancellationToken cancellationToken)
    {
        List<MqttTopicFilter> toSubscribe;
        List<string> toUnsubscribe;
        lock (_subscriptionGate)
        {
            toSubscribe = sessionPresent ? [.. _pendingSubscribe.Values] : [];
            toUnsubscribe = sessionPresent ? [.. _pendingUnsubscribe] : [];
            _pendingSubscribe.Clear();
            _pendingUnsubscribe.Clear();
        }

        if (toSubscribe.Count > 0)
        {
            await raw.SubscribeAsync(toSubscribe, cancellationToken).ConfigureAwait(false);
        }

        if (toUnsubscribe.Count > 0)
        {
            await raw.UnsubscribeAsync(toUnsubscribe, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Serializes <paramref name="value"/> with the configured serializer and publishes it, with
    /// the serializer's content type and payload format stamped on the message.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public Task<PublishOutcome> PublishAsync<T>(
        string topic,
        T value,
        MqttQualityOfService qualityOfService = MqttQualityOfService.AtMostOnce,
        bool retain = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        var serializer = SerializerOrThrow();

        var packet = new MqttPublishPacket
        {
            Topic = topic,
            Payload = serializer.Serialize(value),
            QualityOfService = qualityOfService,
            Retain = retain,
            ContentType = serializer.ContentType,
            PayloadFormatIndicator = serializer.PayloadFormat,
        };
        return PublishAsync(packet, cancellationToken);
    }

    /// <summary>
    /// Registers a typed handler for a route template: payloads are deserialized with the
    /// configured serializer before the handler runs. Deserialization failures surface through
    /// <see cref="MqttRouter.HandlerFaulted"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public IDisposable RegisterRoute<T>(
        MqttRouteTemplate template,
        MqttTypedRouteHandler<T> handler) =>
        RegisterRoute(template, handler, options: null);

    /// <summary>
    /// Registers a typed handler for a route template: payloads are deserialized with the
    /// configured serializer before the handler runs. Deserialization failures surface through
    /// <see cref="MqttRouter.HandlerFaulted"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public IDisposable RegisterRoute<T>(
        MqttRouteTemplate template,
        MqttTypedRouteHandler<T> handler,
        MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(handler);
        var serializer = SerializerOrThrow();

        return RegisterRoute(
            template,
            (message, values, token) =>
                handler(serializer.Deserialize<T>(message.Payload), new MqttRoutedMessage(message, values), token),
            options);
    }

    /// <summary>
    /// Registers a typed local handler for a route template given as text. Payloads are deserialized
    /// with the configured serializer before the handler runs.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public IDisposable RegisterRoute<T>(
        string template,
        MqttTypedRouteHandler<T> handler) =>
        RegisterRoute(MqttRouteTemplate.Parse(template), handler, options: null);

    /// <summary>
    /// Registers a typed local handler for a route template given as text. Payloads are deserialized
    /// with the configured serializer before the handler runs.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public IDisposable RegisterRoute<T>(
        string template,
        MqttTypedRouteHandler<T> handler,
        MqttRouteOptions? options) =>
        RegisterRoute(MqttRouteTemplate.Parse(template), handler, options);

    /// <summary>
    /// Sends a request and awaits the matching response. The client assigns the response topic
    /// (<c>pulse-rpc/&lt;clientId&gt;/&lt;correlation&gt;</c>) and correlation data; the responder must
    /// publish its answer to the request's response topic.
    /// </summary>
    /// <exception cref="MqttException">The client is offline, or no response arrived in time.</exception>
    public async Task<MqttPublishPacket> RequestAsync(
        MqttPublishPacket request,
        MqttRequestOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        EnsureRequestResponseSupported();
        if (request.ResponseTopic is not null || request.CorrelationData is not null)
        {
            throw new ArgumentException("The client assigns the response topic and correlation data.", nameof(request));
        }

        var requestOptions = options ?? new MqttRequestOptions();
        await EnsureResponseRouteAsync(cancellationToken).ConfigureAwait(false);

        var correlation = Guid.NewGuid().ToString("N");
        var pending = new TaskCompletionSource<MqttPublishPacket>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[correlation] = pending;

        try
        {
            var outgoing = request with
            {
                QualityOfService = requestOptions.QualityOfService,
                ResponseTopic = $"pulse-rpc/{_clientId}/{correlation}",
                CorrelationData = Encoding.UTF8.GetBytes(correlation),
            };

            var outcome = await PublishAsync(outgoing, cancellationToken).ConfigureAwait(false);
            if (outcome.Disposition != PublishDisposition.Delivered)
            {
                throw new MqttException("The request could not be delivered: the client is offline.");
            }

            try
            {
                return await pending.Task
                    .WaitAsync(requestOptions.Timeout, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                throw new MqttException($"No response arrived within {requestOptions.Timeout}.");
            }
        }
        finally
        {
            _pendingRequests.TryRemove(correlation, out _);
        }
    }

    /// <summary>Sends a typed request and returns the typed response, using the configured serializer.</summary>
    public async Task<TResponse> RequestAsync<TRequest, TResponse>(
        string topic,
        TRequest request,
        MqttRequestOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        var serializer = SerializerOrThrow();

        var packet = new MqttPublishPacket
        {
            Topic = topic,
            Payload = serializer.Serialize(request),
            ContentType = serializer.ContentType,
            PayloadFormatIndicator = serializer.PayloadFormat,
        };

        var response = await RequestAsync(packet, options, cancellationToken).ConfigureAwait(false);
        return serializer.Deserialize<TResponse>(response.Payload);
    }

    /// <summary>
    /// Sends a request and streams the correlated responses as they arrive, until the responder
    /// publishes the end-of-stream marker, the idle timeout elapses between responses, or the
    /// enumeration is cancelled. The client assigns the response topic and correlation data; the
    /// responder publishes each answer to the request's response topic (see
    /// <c>RegisterRequestStreamHandler</c>). Backpressure is bounded: a slow
    /// consumer throttles delivery rather than buffering without limit.
    /// </summary>
    /// <exception cref="MqttException">The client is offline, or no response arrived within the idle timeout.</exception>
    public async IAsyncEnumerable<MqttPublishPacket> RequestStreamAsync(
        MqttPublishPacket request,
        MqttRequestStreamOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        EnsureRequestResponseSupported();
        if (request.ResponseTopic is not null || request.CorrelationData is not null)
        {
            throw new ArgumentException("The client assigns the response topic and correlation data.", nameof(request));
        }

        var streamOptions = options ?? new MqttRequestStreamOptions();
        await EnsureResponseRouteAsync(cancellationToken).ConfigureAwait(false);

        var correlation = Guid.NewGuid().ToString("N");
        var channel = Channel.CreateBounded<MqttPublishPacket>(new BoundedChannelOptions(streamOptions.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var sink = new StreamSink(channel.Writer);
        _pendingStreams[correlation] = sink;

        try
        {
            var outgoing = request with
            {
                QualityOfService = streamOptions.QualityOfService,
                ResponseTopic = $"pulse-rpc/{_clientId}/{correlation}",
                CorrelationData = Encoding.UTF8.GetBytes(correlation),
            };

            var outcome = await PublishAsync(outgoing, cancellationToken).ConfigureAwait(false);
            if (outcome.Disposition != PublishDisposition.Delivered)
            {
                throw new MqttException("The request could not be delivered: the client is offline.");
            }

            while (true)
            {
                MqttPublishPacket response;
                var closed = false;

                // A per-read timeout that the TimeProvider drives (so it is testable) and that
                // cancels the read itself — the read is awaited directly, so nothing is left dangling.
                using var idle = new CancellationTokenSource(streamOptions.IdleTimeout, _time);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, idle.Token);
                try
                {
                    response = await channel.Reader.ReadAsync(linked.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (idle.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    throw new MqttException($"No stream response arrived within {streamOptions.IdleTimeout}.");
                }
                catch (ChannelClosedException)
                {
                    closed = true;
                    response = null!;
                }

                if (closed)
                {
                    if (sink.Overflowed)
                    {
                        throw new MqttException("The request stream overflowed: the consumer fell behind the configured capacity.");
                    }

                    yield break;
                }

                if (IsEndOfStream(response))
                {
                    yield break;
                }

                yield return response;
            }
        }
        finally
        {
            _pendingStreams.TryRemove(correlation, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>Sends a typed request and streams the typed responses, using the configured serializer.</summary>
    public async IAsyncEnumerable<TResponse> RequestStreamAsync<TRequest, TResponse>(
        string topic,
        TRequest request,
        MqttRequestStreamOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        var serializer = SerializerOrThrow();

        var packet = new MqttPublishPacket
        {
            Topic = topic,
            Payload = serializer.Serialize(request),
            ContentType = serializer.ContentType,
            PayloadFormatIndicator = serializer.PayloadFormat,
        };

        await foreach (var response in RequestStreamAsync(packet, options, cancellationToken).ConfigureAwait(false))
        {
            yield return serializer.Deserialize<TResponse>(response.Payload);
        }
    }

    /// <summary>
    /// Registers a typed streaming responder: each request's handler yields a sequence of
    /// responses, every one published to the request's response topic with its correlation echoed,
    /// followed by the end-of-stream marker. Requests without a response topic are ignored.
    /// </summary>
    public IDisposable RegisterRequestStreamHandler<TRequest, TResponse>(
        MqttRouteTemplate template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, IAsyncEnumerable<TResponse>> handler) =>
        RegisterRequestStreamHandler<TRequest, TResponse>(template, handler, options: null);

    /// <summary>
    /// Registers a typed streaming responder: each request's handler yields a sequence of
    /// responses, every one published to the request's response topic with its correlation echoed,
    /// followed by the end-of-stream marker. Requests without a response topic are ignored.
    /// </summary>
    public IDisposable RegisterRequestStreamHandler<TRequest, TResponse>(
        MqttRouteTemplate template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, IAsyncEnumerable<TResponse>> handler,
        MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(handler);
        EnsureRequestResponseSupported();
        var serializer = SerializerOrThrow();

        return RegisterRoute(
            template,
            async (message, values, token) =>
            {
                if (message.ResponseTopic is not { } responseTopic)
                {
                    return;
                }

                var request = serializer.Deserialize<TRequest>(message.Payload);
                await foreach (var item in handler(request, new MqttRoutedMessage(message, values), token).ConfigureAwait(false))
                {
                    await PublishAsync(
                        new MqttPublishPacket
                        {
                            Topic = responseTopic,
                            Payload = serializer.Serialize(item),
                            QualityOfService = message.QualityOfService,
                            ContentType = serializer.ContentType,
                            PayloadFormatIndicator = serializer.PayloadFormat,
                            CorrelationData = message.CorrelationData,
                        },
                        token).ConfigureAwait(false);
                }

                await PublishAsync(
                    EndOfStreamMarker(responseTopic, message.CorrelationData, message.QualityOfService),
                    token).ConfigureAwait(false);
            },
            options);
    }

    /// <summary>
    /// Registers a typed responder for a route template: each request is deserialized, handled,
    /// and the response published to the request's response topic with its correlation data
    /// echoed. Requests without a response topic are ignored.
    /// </summary>
    public IDisposable RegisterRequestStreamHandler<TRequest, TResponse>(
        string template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, IAsyncEnumerable<TResponse>> handler) =>
        RegisterRequestStreamHandler<TRequest, TResponse>(MqttRouteTemplate.Parse(template), handler, options: null);

    /// <summary>
    /// Registers a typed local streaming responder route given as text. Subscribe the request
    /// filter separately.
    /// </summary>
    public IDisposable RegisterRequestStreamHandler<TRequest, TResponse>(
        string template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, IAsyncEnumerable<TResponse>> handler,
        MqttRouteOptions? options) =>
        RegisterRequestStreamHandler(MqttRouteTemplate.Parse(template), handler, options);

    /// <summary>
    /// Registers a typed local responder route: each request is deserialized, handled, and the
    /// response published to the request's response topic with its correlation data echoed.
    /// Requests without a response topic are ignored. Subscribe the request filter separately.
    /// </summary>
    public IDisposable RegisterRequestHandler<TRequest, TResponse>(
        MqttRouteTemplate template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, ValueTask<TResponse>> handler) =>
        RegisterRequestHandler<TRequest, TResponse>(template, handler, options: null);

    /// <summary>
    /// Registers a typed local responder route: each request is deserialized, handled, and the
    /// response published to the request's response topic with its correlation data echoed.
    /// Requests without a response topic are ignored. Subscribe the request filter separately.
    /// </summary>
    public IDisposable RegisterRequestHandler<TRequest, TResponse>(
        MqttRouteTemplate template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, ValueTask<TResponse>> handler,
        MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(handler);
        EnsureRequestResponseSupported();
        var serializer = SerializerOrThrow();

        return RegisterRoute(
            template,
            async (message, values, token) =>
            {
                if (message.ResponseTopic is not { } responseTopic)
                {
                    return;
                }

                var request = serializer.Deserialize<TRequest>(message.Payload);
                var response = await handler(request, new MqttRoutedMessage(message, values), token).ConfigureAwait(false);

                var reply = new MqttPublishPacket
                {
                    Topic = responseTopic,
                    Payload = serializer.Serialize(response),
                    QualityOfService = message.QualityOfService,
                    ContentType = serializer.ContentType,
                    PayloadFormatIndicator = serializer.PayloadFormat,
                    CorrelationData = message.CorrelationData,
                };
                await PublishAsync(reply, token).ConfigureAwait(false);
            },
            options);
    }

    /// <summary>
    /// Registers a typed local responder route given as text. Subscribe the request filter separately.
    /// </summary>
    public IDisposable RegisterRequestHandler<TRequest, TResponse>(
        string template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, ValueTask<TResponse>> handler) =>
        RegisterRequestHandler<TRequest, TResponse>(MqttRouteTemplate.Parse(template), handler, options: null);

    /// <summary>
    /// Registers a typed local responder route given as text. Subscribe the request filter separately.
    /// </summary>
    public IDisposable RegisterRequestHandler<TRequest, TResponse>(
        string template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, ValueTask<TResponse>> handler,
        MqttRouteOptions? options) =>
        RegisterRequestHandler(MqttRouteTemplate.Parse(template), handler, options);

    private Task EnsureResponseRouteAsync(CancellationToken cancellationToken)
    {
        lock (_rpcGate)
        {
            _rpcRoute ??= EnsureResponseRouteCoreAsync(cancellationToken);
            return _rpcRoute;
        }
    }

    private async Task EnsureResponseRouteCoreAsync(CancellationToken cancellationToken)
    {
        EnsureRequestResponseSupported();
        var template = MqttRouteTemplate.Parse($"pulse-rpc/{_clientId}/{{correlation}}");
        await SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], cancellationToken).ConfigureAwait(false);
        RegisterRoute(
            template,
            (message, values, _) =>
            {
                var correlation = values["correlation"];
                if (_pendingRequests.TryGetValue(correlation, out var pending))
                {
                    pending.TrySetResult(message);
                }
                else if (_pendingStreams.TryGetValue(correlation, out var sink) && !sink.Writer.TryWrite(message))
                {
                    // Non-blocking: never stall the shared dispatch loop on a slow stream
                    // consumer. A full buffer means the consumer fell behind its capacity, so
                    // fail that one stream explicitly rather than block the client or drop
                    // silently.
                    sink.Overflowed = true;
                    sink.Writer.TryComplete();
                }

                return ValueTask.CompletedTask;
            });
    }

    internal IMqttSerializer SerializerOrThrow() =>
        _options.Serializer
        ?? throw new InvalidOperationException("Configure a serializer in the options to use typed messaging.");

    private void EnsureRequestResponseSupported()
    {
        if (!CanUseProtocolFeature(MqttProtocolFeature.RequestResponse))
        {
            throw new NotSupportedException(
                "Request/response requires MQTT 5.0 because it uses response-topic and correlation-data publish properties. MQTT 3.1.1 applications should use an explicit payload or topic convention.");
        }
    }

    private static bool IsBrokerNegotiatedFeature(MqttProtocolFeature feature) =>
        feature is MqttProtocolFeature.TopicAliases
            or MqttProtocolFeature.SubscriptionIdentifiers
            or MqttProtocolFeature.SharedSubscriptions;

    // One open request-stream's delivery target: the channel the route handler writes responses to,
    // plus a flag set when a full buffer forced the stream to fail rather than block or drop.
    private sealed class StreamSink(ChannelWriter<MqttPublishPacket> writer)
    {
        public ChannelWriter<MqttPublishPacket> Writer { get; } = writer;

        public volatile bool Overflowed;
    }

    // The user property that marks the final message of a streamed response. The marker carries no
    // payload; it tells the consumer the sequence is complete.
    private const string EndOfStreamProperty = "pulse.eos";

    private static MqttPublishPacket EndOfStreamMarker(string topic, ReadOnlyMemory<byte>? correlation, MqttQualityOfService qualityOfService) => new()
    {
        Topic = topic,
        QualityOfService = qualityOfService,
        CorrelationData = correlation,
        UserProperties = [new MqttUserProperty(EndOfStreamProperty, "true")],
    };

    private static bool IsEndOfStream(MqttPublishPacket message)
    {
        foreach (var property in message.UserProperties)
        {
            if (property.Name == EndOfStreamProperty)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Streams state transitions. Late subscribers see transitions from subscription onward.</summary>
    public async IAsyncEnumerable<ConnectionStateChanged> WatchState(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var watcher = Channel.CreateBounded<ConnectionStateChanged>(new BoundedChannelOptions(64)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        lock (_stateGate)
        {
            _watchers.Add(watcher);
        }

        try
        {
            await foreach (var change in watcher.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return change;
            }
        }
        finally
        {
            lock (_stateGate)
            {
                _watchers.Remove(watcher);
            }
        }
    }

    /// <summary>
    /// Waits until the client reaches <see cref="ConnectionState.Connected"/> or throws if it
    /// faults first. If the state is already connected, the returned task completes immediately.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="timeout"/> is negative and not <see cref="Timeout.InfiniteTimeSpan"/>.
    /// </exception>
    /// <exception cref="TimeoutException">The client did not connect before <paramref name="timeout"/> elapsed.</exception>
    /// <exception cref="InvalidOperationException">The client reached <see cref="ConnectionState.Faulted"/>.</exception>
    public async Task WaitUntilConnectedAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (timeout < TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The timeout must be non-negative or InfiniteTimeSpan.");
        }

        var watcher = Channel.CreateBounded<ConnectionStateChanged>(new BoundedChannelOptions(64)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        lock (_stateGate)
        {
            if (State == ConnectionState.Connected)
            {
                return;
            }

            if (State == ConnectionState.Faulted)
            {
                throw new InvalidOperationException("The MQTT client is faulted.");
            }

            _watchers.Add(watcher);
        }

        CancellationTokenSource? timeoutSource = null;
        CancellationTokenSource? linkedSource = null;
        try
        {
            timeoutSource = timeout == Timeout.InfiniteTimeSpan ? null : new CancellationTokenSource(timeout, _time);
            linkedSource = timeoutSource is null
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);

            await foreach (var change in watcher.Reader.ReadAllAsync(linkedSource.Token).ConfigureAwait(false))
            {
                if (change.Current == ConnectionState.Connected)
                {
                    return;
                }

                if (change.Current == ConnectionState.Faulted)
                {
                    throw new InvalidOperationException("The MQTT client is faulted.");
                }
            }
        }
        catch (OperationCanceledException) when (timeoutSource?.IsCancellationRequested == true && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"The MQTT client did not reach Connected within {timeout}.");
        }
        finally
        {
            linkedSource?.Dispose();
            timeoutSource?.Dispose();
            lock (_stateGate)
            {
                _watchers.Remove(watcher);
            }
        }
    }

    /// <summary>
    /// Starts a client-initiated re-authentication on the live connection (requires an
    /// authenticator on <see cref="RawMqttClientOptions.Authenticator"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException">The client is not connected, or no authenticator is configured.</exception>
    public Task ReAuthenticateAsync(CancellationToken cancellationToken = default) =>
        (_raw ?? throw new InvalidOperationException("The client is not connected."))
            .ReAuthenticateAsync(cancellationToken);

    /// <summary>Disconnects this resilient client and stops reconnecting.</summary>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (_lifetime is { } lifetime)
        {
            await lifetime.CancelAsync().ConfigureAwait(false);
        }

        if (_supervisor is { } supervisor)
        {
            await supervisor.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        if (State != ConnectionState.Stopped)
        {
            Transition(ConnectionState.Stopped);
        }
    }

    /// <summary>
    /// Stops the supervisor and closes any live connection. Prefer <see cref="DisconnectAsync"/>
    /// for application code; this alias remains for source compatibility with earlier releases.
    /// </summary>
    [Obsolete("Use DisconnectAsync. StopAsync is retained only as a compatibility alias.")]
    public Task StopAsync(CancellationToken cancellationToken = default) => DisconnectAsync(cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        GC.SuppressFinalize(this);
        PulseMqttDiagnostics.UnregisterOfflineQueue(_offlineProbe);
        await DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
        _messages.Writer.TryComplete();
        if (_router.IsValueCreated)
        {
            await _router.Value.DisposeAsync().ConfigureAwait(false);
        }

        _lifetime?.Dispose();
    }

    private sealed class OfflineQueueProbe(string clientId, IMessageStore store) : PulseMqttDiagnostics.IOfflineQueueProbe
    {
        public string ClientId => clientId;

        public long Depth => store.Count;

        public long Dropped => store.DroppedCount;
    }

    private async Task SuperviseAsync(CancellationToken cancellationToken)
    {
        MqttServerDisconnectedException? serverDrop = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Transition(
                    _attempt == 0 ? ConnectionState.Connecting : ConnectionState.Reconnecting,
                    serverDrop?.ReasonCode,
                    serverDrop?.ReasonString,
                    serverDrop?.ServerReference,
                    serverDrop);
                serverDrop = null;

                RawMqttClient? raw = null;
                MqttConnAckPacket? connAck = null;

                try
                {
                    await _strategy.RunAsync(
                        async token =>
                        {
                            var candidate = new RawMqttClient(_transportFactory, _options.Raw, _time);
                            candidate.AcknowledgedMessageSink = DispatchInboundAsync;

                            async ValueTask DispatchInboundAsync(
                                MqttInboundPublishContext context,
                                CancellationToken sinkToken)
                            {
                                PulseMqttDiagnostics.MessagesReceived.Add(1, new KeyValuePair<string, object?>("client.id", _clientId));
                                if (_router.IsValueCreated &&
                                    await _router.Value.DeliverAcknowledgedAsync(context, sinkToken).ConfigureAwait(false))
                                {
                                    return;
                                }

                                await _messages.Writer.WriteAsync(context.Message, sinkToken).ConfigureAwait(false);
                                await context.AcknowledgeAsync(sinkToken).ConfigureAwait(false);
                            }

                            var clientIdTag = new KeyValuePair<string, object?>("client.id", _clientId);
                            var connectStart = _time.GetTimestamp();
                            using var connectActivity = PulseMqttDiagnostics.ActivitySource.StartActivity("connect", ActivityKind.Client);
                            connectActivity?.SetTag("messaging.system", "mqtt");
                            connectActivity?.SetTag("client.id", _clientId);
                            try
                            {
                                var connect = await BuildConnectForAttemptAsync(token).ConfigureAwait(false);

                                // Held in-flight work is only redeliverable if the broker keeps
                                // the session; capture the count first so a clean session that
                                // discards it (per spec) is observable rather than silent.
                                var heldInFlight = _inFlightSession?.Snapshot().Outbound.Count ?? 0;

                                var ack = await candidate.ConnectAsync(connect, _inFlightSession, token).ConfigureAwait(false);
                                if (ack.ReasonCode != MqttReasonCode.Success)
                                {
                                    throw new MqttConnectRejectedException(
                                        ack.ReasonCode,
                                        ack.ReasonString,
                                        ack.ServerReference);
                                }

                                if (heldInFlight > 0 && !ack.SessionPresent && _options.Logger is { } discardLogger)
                                {
                                    PulseMqttLog.InFlightDiscarded(discardLogger, _clientId, heldInFlight);
                                }

                                connectActivity?.SetTag("pulse.mqtt.session_present", ack.SessionPresent);
                                PulseMqttDiagnostics.ConnectDuration.Record(
                                    _time.GetElapsedTime(connectStart).TotalSeconds,
                                    clientIdTag,
                                    new KeyValuePair<string, object?>("outcome", "success"));

                                raw = candidate;
                                connAck = ack;
                            }
                            catch (Exception error)
                            {
                                connectActivity?.SetStatus(ActivityStatusCode.Error, error.Message);
                                PulseMqttDiagnostics.ConnectDuration.Record(
                                    _time.GetElapsedTime(connectStart).TotalSeconds,
                                    clientIdTag,
                                    new KeyValuePair<string, object?>("outcome", "error"));
                                await candidate.DisposeAsync().ConfigureAwait(false);

                                // A CONNACK-level redirect retargets the factory and rethrows: the
                                // reason is retryable, so the strategy's next attempt already
                                // connects to the referenced server.
                                if (error is MqttConnectRejectedException rejected)
                                {
                                    TryFollowServerRedirect(rejected.ReasonCode, rejected.ServerReference);
                                }

                                throw;
                            }
                        },
                        new SupervisorReconnectContext(this),
                        cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception error)
                {
                    Fault(error);
                    return;
                }

                try
                {
                    var upContext = new ConnectionUpContext(connAck!, _attempt, new RawSubscriptionRegistrar(raw!));
                    await _lifecycle.OnConnectionUpAsync(upContext, cancellationToken).ConfigureAwait(false);

                    // The lifecycle replays the full subscription set only on a fresh session. On a
                    // resumed one (SessionPresent = true) it does nothing, so any subscribe/unsubscribe
                    // made while offline must be applied here or the broker would never hear it.
                    await ReconcileOfflineSubscriptionsAsync(raw!, connAck!.SessionPresent, cancellationToken).ConfigureAwait(false);

                    // Order is deliberate and spec-driven:
                    //   1. re-subscription (above) is in place first,
                    //   2. unacknowledged QoS 1/2 exchanges from the resumed session redeliver,
                    //   3. the birth announces "online",
                    //   4. never-sent offline publishes flush,
                    //   5. the state becomes Connected.
                    // Redelivery precedes the queue flush so resumed in-flight work completes in
                    // its original order before any newly-queued traffic.
                    await raw!.RedeliverAsync(cancellationToken).ConfigureAwait(false);
                    await PublishBirthAsync(raw!, cancellationToken).ConfigureAwait(false);
                    await FlushQueuedGatedAsync(raw!, cancellationToken).ConfigureAwait(false);

                    // A publish racing the previous connection's death can record into the shared
                    // in-flight session after RedeliverAsync's snapshot walked it. No publish can
                    // target this connection before _raw is set below, so anything present now is
                    // that late debris — resume it here instead of stranding it until the next
                    // reconnect.
                    if (_inFlightSession is not null && _inFlightSession.Snapshot().Outbound.Count > 0)
                    {
                        await raw!.RedeliverAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    await raw!.DisposeAsync().ConfigureAwait(false);
                    break;
                }
                catch
                {
                    // The connection died during up-handling; retry the whole cycle.
                    await raw!.DisposeAsync().ConfigureAwait(false);
                    _attempt++;
                    continue;
                }

                _raw = raw;
                Transition(
                    ConnectionState.Connected,
                    brokerCapabilities: MqttBrokerCapabilitiesSnapshot.From(connAck!, _options.Connect));

                try
                {
                    await raw!.Completion.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown; the loop below observes the token.
                }

                _raw = null;
                var serverDisconnect = raw!.ServerDisconnect;
                await raw!.DisposeAsync().ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (_options.Logger is { } logger)
                {
                    if (serverDisconnect is not null)
                    {
                        PulseMqttLog.ServerDisconnected(logger, _clientId, serverDisconnect.ReasonCode);
                    }
                    else
                    {
                        PulseMqttLog.ConnectionLost(logger, _clientId);
                    }
                }

                _attempt++;
                serverDrop = serverDisconnect;
                try
                {
                    await _lifecycle.OnConnectionDownAsync(
                            new ConnectionDownContext(
                                serverDisconnect?.ReasonCode,
                                serverDisconnect?.ReasonString,
                                serverDisconnect?.ServerReference,
                                serverDisconnect),
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                // A broker that said "stop" — a ban, a takeover, a redirect — must not be
                // hammered with reconnects. The decision classifies; terminal reasons fault,
                // unless the reason is a redirect the client is configured to follow.
                if (serverDisconnect is not null && !_decision.ShouldRetry(_attempt, serverDisconnect))
                {
                    if (TryFollowServerRedirect(serverDisconnect.ReasonCode, serverDisconnect.ServerReference))
                    {
                        _attempt++;
                        continue;
                    }

                    Fault(serverDisconnect);
                    return;
                }
            }

            Transition(ConnectionState.Stopped);
        }
        catch (Exception error)
        {
            Fault(error);
        }
    }

    /// <summary>
    /// Retargets the transport at the broker-referenced server when redirect following applies:
    /// the option is on, the reason is a redirect, the reference parses, the factory can be
    /// retargeted, and the consecutive-hop bound is not exhausted.
    /// </summary>
    private bool TryFollowServerRedirect(MqttReasonCode? reason, string? serverReference)
    {
        if (!_options.FollowServerRedirects
            || reason is not (MqttReasonCode.UseAnotherServer or MqttReasonCode.ServerMoved)
            || _transportFactory is not IRedirectableTransportFactory redirectable
            || !MqttServerReference.TryParse(serverReference, out var host, out var port))
        {
            return false;
        }

        // The bound targets rapid ping-pong loops, not occasional rebalancing over a long
        // uptime: a chain that has been quiet renews its budget.
        if (_redirectHops > 0 && _time.GetElapsedTime(_lastRedirectTimestamp) >= RedirectChainQuietPeriod)
        {
            _redirectHops = 0;
        }

        if (_redirectHops >= _options.MaxServerRedirects)
        {
            if (_options.Logger is { } boundLogger)
            {
                PulseMqttLog.ServerRedirectBoundReached(boundLogger, _clientId, _options.MaxServerRedirects);
            }

            return false;
        }

        _redirectHops++;
        _lastRedirectTimestamp = _time.GetTimestamp();
        _transportFactory = redirectable.WithServer(host, port);
        if (_options.Logger is { } logger)
        {
            PulseMqttLog.FollowingServerRedirect(logger, _clientId, host, port, _redirectHops);
        }

        PulseMqttDiagnostics.StateTransitions.Add(
            1,
            new KeyValuePair<string, object?>("client.id", _clientId),
            new KeyValuePair<string, object?>("state", "Redirected"));
        return true;
    }

    private async Task PublishBirthAsync(RawMqttClient raw, CancellationToken cancellationToken)
    {
        var birth = _options.BirthFactory is { } factory
            ? await factory(_attempt, cancellationToken).ConfigureAwait(false)
            : _options.Birth;
        if (birth is null)
        {
            return;
        }

        try
        {
            await raw.PublishAsync(birth, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error) when (_options.BirthFailure == BirthFailurePolicy.LogAndContinue)
        {
            if (_options.Logger is { } logger)
            {
                PulseMqttLog.BirthPublishFailed(logger, _clientId, birth.Topic, error);
            }

            PulseMqttDiagnostics.MessagesPublished.Add(
                1,
                new KeyValuePair<string, object?>("client.id", _clientId),
                new KeyValuePair<string, object?>("disposition", "BirthFailed"));
        }
    }

    private async ValueTask<MqttConnectPacket> BuildConnectForAttemptAsync(CancellationToken cancellationToken)
    {
        var connect = _options.Connect;
        if (_options.WillProvider is { } provider)
        {
            var context = new MqttWillContext(
                _clientId,
                _attempt,
                connect.ProtocolVersion,
                connect.CleanStart,
                connect.KeepAliveSeconds,
                _time.GetUtcNow());
            return connect with
            {
                Will = await provider.CreateWillAsync(context, cancellationToken).ConfigureAwait(false),
            };
        }

        if (_options.WillFactory is { } willFactory)
        {
            return connect with { Will = await willFactory(cancellationToken).ConfigureAwait(false) };
        }

        return _options.Will is { } will
            ? connect with { Will = will }
            : connect;
    }

    private async Task FlushQueuedAsync(RawMqttClient raw, CancellationToken cancellationToken)
    {
        while (await _messageStore.PeekQueuedAsync(cancellationToken).ConfigureAwait(false) is { } entry)
        {
            var queued = entry.Packet;

            // MQTT 5 message expiry counts the time spent waiting: a message that outlived its
            // expiry in the offline queue must not be delivered at all, and one still alive goes
            // out with the wait subtracted from its remaining interval (rounded up, so a message
            // that has not expired never leaves with zero).
            if (queued.MessageExpiryInterval is { } expirySeconds && entry.EnqueuedAt is { } enqueuedAt)
            {
                var remaining = TimeSpan.FromSeconds(expirySeconds) - (_time.GetUtcNow() - enqueuedAt);
                if (remaining <= TimeSpan.Zero)
                {
                    if (_options.Logger is { } expiryLogger)
                    {
                        PulseMqttLog.QueuedPublishExpired(expiryLogger, _clientId, queued.Topic, expirySeconds);
                    }

                    PulseMqttDiagnostics.MessagesPublished.Add(
                        1,
                        new KeyValuePair<string, object?>("client.id", _clientId),
                        new KeyValuePair<string, object?>("disposition", "DroppedExpired"));
                    await _messageStore.RemoveHeadAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                queued = queued with { MessageExpiryInterval = (uint)Math.Ceiling(remaining.TotalSeconds) };
            }

            try
            {
                await raw.PublishAsync(queued, cancellationToken).ConfigureAwait(false);
            }
            catch (MqttPacketTooLargeException error)
            {
                // This broker accepts smaller packets than the one that queued the message.
                // Retrying can never succeed; drop it loudly and keep the queue draining.
                if (_options.Logger is { } logger)
                {
                    PulseMqttLog.QueuedPublishTooLarge(logger, _clientId, queued.Topic, error.PacketSize, error.Limit);
                }

                PulseMqttDiagnostics.MessagesPublished.Add(
                    1,
                    new KeyValuePair<string, object?>("client.id", _clientId),
                    new KeyValuePair<string, object?>("disposition", "DroppedTooLarge"));
            }

            await _messageStore.RemoveHeadAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void Fault(Exception error)
    {
        var details = TransitionDetails.From(error);
        Transition(ConnectionState.Faulted, details.Reason, details.ReasonString, details.ServerReference, error);
    }

    private sealed record ConnectionDownContext(
        MqttReasonCode? Reason,
        string? ReasonString,
        string? ServerReference,
        Exception? Error) : IConnectionDownContext;

    private void Transition(
        ConnectionState next,
        MqttReasonCode? reason = null,
        string? reasonString = null,
        string? serverReference = null,
        Exception? error = null,
        MqttBrokerCapabilitiesSnapshot? brokerCapabilities = null)
    {
        ConnectionStateChanged changed;
        Channel<ConnectionStateChanged>[] watchers;
        lock (_stateGate)
        {
            if (next == ConnectionState.Connected)
            {
                reason = null;
                reasonString = null;
                serverReference = null;
                error = null;
                _brokerCapabilities = brokerCapabilities;
            }
            else
            {
                _brokerCapabilities = null;
            }

            changed = new ConnectionStateChanged(State, next, _attempt, reason)
            {
                ReasonString = reasonString,
                ServerReference = serverReference,
                Error = error,
            };

            State = next;
            _stateChangedAt = _time.GetUtcNow();
            _lastReason = reason;
            _lastReasonString = reasonString;
            _lastServerReference = serverReference;
            _lastError = error;
            watchers = [.. _watchers];
        }

        if (_options.Logger is { } logger)
        {
            PulseMqttLog.StateChanged(logger, _clientId, changed.Previous, changed.Current, changed.Attempt);
        }

        PulseMqttDiagnostics.StateTransitions.Add(
            1,
            new KeyValuePair<string, object?>("client.id", _clientId),
            new KeyValuePair<string, object?>("state", next.ToString()));

        StateChanged?.Invoke(changed);
        foreach (var watcher in watchers)
        {
            watcher.Writer.TryWrite(changed);
        }
    }

    private sealed class SupervisorReconnectContext(ResilientMqttClient client) : IReconnectContext
    {
        public int Attempt { get; private set; }

        public TimeProvider Time => client._time;

        public void OnAttemptStarting(int attempt)
        {
            Attempt = attempt;
            PulseMqttDiagnostics.ConnectAttempts.Add(1, new KeyValuePair<string, object?>("client.id", client._clientId));
            if (attempt > 1)
            {
                client.Transition(client._attempt == 0 ? ConnectionState.Connecting : ConnectionState.Reconnecting);
            }
        }

        public void OnAttemptFailed(int attempt, Exception error)
        {
            if (client._options.Logger is { } logger)
            {
                PulseMqttLog.ConnectAttemptFailed(logger, client._clientId, attempt, error);
            }

            var details = TransitionDetails.From(error);
            client.Transition(ConnectionState.WaitingRetry, details.Reason, details.ReasonString, details.ServerReference, error);
        }
    }

    private static (int? Depth, long? DroppedCount) ReadOfflineQueueCounters(IMessageStore store)
    {
        try
        {
            return (store.Count, store.DroppedCount);
        }
        catch (Exception)
        {
            return (null, null);
        }
    }

    private readonly record struct TransitionDetails(
        MqttReasonCode? Reason,
        string? ReasonString,
        string? ServerReference)
    {
        public static TransitionDetails From(Exception error)
        {
            var rejected = Find<MqttConnectRejectedException>(error);
            if (rejected is not null)
            {
                return new TransitionDetails(
                    rejected.ReasonCode,
                    rejected.ReasonString,
                    rejected.ServerReference);
            }

            var serverDisconnected = Find<MqttServerDisconnectedException>(error);
            if (serverDisconnected is not null)
            {
                return new TransitionDetails(
                    serverDisconnected.ReasonCode,
                    serverDisconnected.ReasonString,
                    serverDisconnected.ServerReference);
            }

            return default;
        }

        private static T? Find<T>(Exception? error)
            where T : Exception
        {
            while (error is not null)
            {
                if (error is T typed)
                {
                    return typed;
                }

                error = error.InnerException;
            }

            return null;
        }
    }

    private sealed class ConnectionUpContext(MqttConnAckPacket connAck, int attempt, ISubscriptionRegistrar registrar)
        : IConnectionUpContext
    {
        public MqttConnAckPacket ConnAck => connAck;

        public int Attempt => attempt;

        public ISubscriptionRegistrar Subscriptions => registrar;
    }

    private sealed class RawSubscriptionRegistrar(RawMqttClient raw) : ISubscriptionRegistrar
    {
        public async ValueTask<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
            IReadOnlyList<MqttTopicFilter> topicFilters,
            CancellationToken cancellationToken) =>
            await raw.SubscribeAsync(topicFilters, cancellationToken).ConfigureAwait(false);
    }
}
