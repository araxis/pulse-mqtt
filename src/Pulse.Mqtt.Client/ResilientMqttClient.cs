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
    private readonly IMqttTransportFactory _transportFactory;
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
    private readonly object _stateGate = new();
    private readonly object _subscriptionGate = new();

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

    /// <summary>Starts the supervisor. Connection happens in the background; watch <see cref="State"/>.</summary>
    /// <exception cref="InvalidOperationException">The client is already running.</exception>
    public async Task StartAsync(CancellationToken cancellationToken)
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
    /// Publishes through the live connection when available; otherwise queues (QoS &gt; 0, or QoS 0
    /// when configured) or drops QoS 0 — always explicitly, never silently.
    /// </summary>
    public async Task<PublishOutcome> PublishAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
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
        if (_options.PropagateTraceContext && Activity.Current is { } current)
        {
            packet = TraceContextPropagation.Inject(packet, current.Context);
        }

        var start = _time.GetTimestamp();
        var outcome = await PublishCoreAsync(packet, cancellationToken).ConfigureAwait(false);
        var elapsed = _time.GetElapsedTime(start).TotalSeconds;

        var clientIdTag = new KeyValuePair<string, object?>("client.id", _clientId);
        var dispositionTag = new KeyValuePair<string, object?>("disposition", outcome.Disposition.ToString());
        activity?.SetTag("pulse.mqtt.disposition", outcome.Disposition.ToString());
        PulseMqttDiagnostics.MessagesPublished.Add(1, clientIdTag, dispositionTag);
        PulseMqttDiagnostics.PublishDuration.Record(elapsed, clientIdTag, dispositionTag);
        return outcome;
    }

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

        await _messageStore.EnqueueAsync(packet, cancellationToken).ConfigureAwait(false);
        return new PublishOutcome(PublishDisposition.Queued);
    }

    /// <summary>
    /// Adds subscriptions to the durable set and applies them on the live connection when one
    /// exists. Offline, they apply on the next connection-up; the result list is then empty.
    /// </summary>
    public async Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        IReadOnlyList<MqttTopicFilter> topicFilters,
        CancellationToken cancellationToken)
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
                return await raw.SubscribeAsync(topicFilters, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
            {
                // The connection died; the lifecycle re-subscribes on the next connection-up.
            }
        }

        return [];
    }

    /// <summary>Removes subscriptions from the durable set and from the live connection when one exists.</summary>
    public async Task<IReadOnlyList<MqttReasonCode>> UnsubscribeAsync(
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken)
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
                return await raw.UnsubscribeAsync(topicFilters, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
            {
            }
        }

        return [];
    }

    /// <summary>
    /// Registers a handler for a route template (for example <c>sensors/{deviceId}/temp</c>) and
    /// subscribes to the template's topic filter. Dispose the registration to remove the route;
    /// the subscription stays until <see cref="UnsubscribeAsync"/>.
    /// </summary>
    public async Task<IDisposable> OnAsync(
        string template,
        MqttRouteHandler handler,
        MqttRouteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var parsed = MqttRouteTemplate.Parse(template);
        var registration = Router.On(parsed, handler, options);
        await SubscribeToTemplateAsync(parsed, options, cancellationToken).ConfigureAwait(false);
        return registration;
    }

    /// <summary>Opens an <c>await foreach</c>-able stream for a route template and subscribes to its filter.</summary>
    public async Task<MqttRouteStream> OpenStreamAsync(
        string template,
        MqttRouteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var parsed = MqttRouteTemplate.Parse(template);
        var stream = Router.OpenStream(parsed, options);
        await SubscribeToTemplateAsync(parsed, options, cancellationToken).ConfigureAwait(false);
        return stream;
    }

    private Task SubscribeToTemplateAsync(MqttRouteTemplate template, MqttRouteOptions? options, CancellationToken cancellationToken)
    {
        var filter = new MqttTopicFilter(template.TopicFilter)
        {
            MaximumQualityOfService = (options ?? new MqttRouteOptions()).SubscriptionQualityOfService,
        };
        return SubscribeAsync([filter], cancellationToken);
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
    public Task<IDisposable> OnAsync<T>(
        string template,
        MqttTypedRouteHandler<T> handler,
        MqttRouteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var serializer = SerializerOrThrow();

        return OnAsync(
            template,
            (message, values, token) =>
                handler(serializer.Deserialize<T>(message.Payload), new MqttRoutedMessage(message, values), token),
            options,
            cancellationToken);
    }

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
    /// <see cref="OnRequestStreamAsync{TRequest, TResponse}"/>). Backpressure is bounded: a slow
    /// consumer throttles delivery rather than buffering without limit.
    /// </summary>
    /// <exception cref="MqttException">The client is offline, or no response arrived within the idle timeout.</exception>
    public async IAsyncEnumerable<MqttPublishPacket> RequestStreamAsync(
        MqttPublishPacket request,
        MqttRequestStreamOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
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
    public Task<IDisposable> OnRequestStreamAsync<TRequest, TResponse>(
        string template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, IAsyncEnumerable<TResponse>> handler,
        MqttRouteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var serializer = SerializerOrThrow();

        return OnAsync(
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
            options,
            cancellationToken);
    }

    /// <summary>
    /// Registers a typed responder for a route template: each request is deserialized, handled,
    /// and the response published to the request's response topic with its correlation data
    /// echoed. Requests without a response topic are ignored.
    /// </summary>
    public Task<IDisposable> OnRequestAsync<TRequest, TResponse>(
        string template,
        Func<TRequest, MqttRoutedMessage, CancellationToken, ValueTask<TResponse>> handler,
        MqttRouteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var serializer = SerializerOrThrow();

        return OnAsync(
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
            options,
            cancellationToken);
    }

    private Task EnsureResponseRouteAsync(CancellationToken cancellationToken)
    {
        lock (_rpcGate)
        {
            _rpcRoute ??= OnAsync(
                $"pulse-rpc/{_clientId}/{{correlation}}",
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
                },
                cancellationToken: cancellationToken);
            return _rpcRoute;
        }
    }

    internal IMqttSerializer SerializerOrThrow() =>
        _options.Serializer
        ?? throw new InvalidOperationException("Configure a serializer in the options to use typed messaging.");

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
    /// Starts a client-initiated re-authentication on the live connection (requires an
    /// authenticator on <see cref="RawMqttClientOptions.Authenticator"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException">The client is not connected, or no authenticator is configured.</exception>
    public Task ReAuthenticateAsync(CancellationToken cancellationToken = default) =>
        (_raw ?? throw new InvalidOperationException("The client is not connected."))
            .ReAuthenticateAsync(cancellationToken);

    /// <summary>Stops the supervisor and closes any live connection.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
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
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
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
        MqttReasonCode? dropReason = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Transition(_attempt == 0 ? ConnectionState.Connecting : ConnectionState.Reconnecting, dropReason);
                dropReason = null;

                RawMqttClient? raw = null;
                MqttConnAckPacket? connAck = null;

                try
                {
                    await _strategy.RunAsync(
                        async token =>
                        {
                            var candidate = new RawMqttClient(_transportFactory, _options.Raw, _time);
                            candidate.MessageSink = (message, sinkToken) =>
                            {
                                PulseMqttDiagnostics.MessagesReceived.Add(1, new KeyValuePair<string, object?>("client.id", _clientId));
                                return _messages.Writer.WriteAsync(message, sinkToken);
                            };

                            var clientIdTag = new KeyValuePair<string, object?>("client.id", _clientId);
                            var connectStart = _time.GetTimestamp();
                            using var connectActivity = PulseMqttDiagnostics.ActivitySource.StartActivity("connect", ActivityKind.Client);
                            connectActivity?.SetTag("messaging.system", "mqtt");
                            connectActivity?.SetTag("client.id", _clientId);
                            try
                            {
                                // The will is computed per attempt: a factory produces a fresh
                                // topic and payload for every reconnect, and a throwing factory
                                // fails this attempt like any other connect failure.
                                var connect = _options.Connect;
                                if (_options.WillFactory is { } willFactory)
                                {
                                    connect = connect with { Will = await willFactory(token).ConfigureAwait(false) };
                                }
                                else if (_options.Will is { } will)
                                {
                                    connect = connect with { Will = will };
                                }

                                // Held in-flight work is only redeliverable if the broker keeps
                                // the session; capture the count first so a clean session that
                                // discards it (per spec) is observable rather than silent.
                                var heldInFlight = _inFlightSession?.Snapshot().Outbound.Count ?? 0;

                                var ack = await candidate.ConnectAsync(connect, _inFlightSession, token).ConfigureAwait(false);
                                if (ack.ReasonCode != MqttReasonCode.Success)
                                {
                                    throw new MqttConnectRejectedException(ack.ReasonCode);
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
                    await FlushQueuedAsync(raw!, cancellationToken).ConfigureAwait(false);
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
                Transition(ConnectionState.Connected);

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
                dropReason = serverDisconnect?.ReasonCode;
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
                // hammered with reconnects. The decision classifies; terminal reasons fault.
                if (serverDisconnect is not null && !_decision.ShouldRetry(_attempt, serverDisconnect))
                {
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

    private async Task FlushQueuedAsync(RawMqttClient raw, CancellationToken cancellationToken)
    {
        while (await _messageStore.PeekAsync(cancellationToken).ConfigureAwait(false) is { } queued)
        {
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
        var reason = (error as MqttConnectRejectedException ?? error.InnerException as MqttConnectRejectedException)?.ReasonCode
            ?? (error as MqttServerDisconnectedException ?? error.InnerException as MqttServerDisconnectedException)?.ReasonCode;
        Transition(ConnectionState.Faulted, reason);
    }

    private sealed record ConnectionDownContext(
        MqttReasonCode? Reason,
        string? ReasonString,
        string? ServerReference,
        Exception? Error) : IConnectionDownContext;

    private void Transition(ConnectionState next, MqttReasonCode? reason = null)
    {
        ConnectionStateChanged changed;
        Channel<ConnectionStateChanged>[] watchers;
        lock (_stateGate)
        {
            changed = new ConnectionStateChanged(State, next, _attempt, reason);
            State = next;
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

            var reason = (error as MqttConnectRejectedException)?.ReasonCode;
            client.Transition(ConnectionState.WaitingRetry, reason);
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
