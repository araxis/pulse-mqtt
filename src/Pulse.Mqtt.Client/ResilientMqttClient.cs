using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
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
    private readonly Channel<MqttPublishPacket> _messages;
    private readonly List<Channel<ConnectionStateChanged>> _watchers = [];
    private readonly List<MqttTopicFilter> _subscriptions = [];
    private readonly object _stateGate = new();
    private readonly object _subscriptionGate = new();

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
        var decision = options.ReconnectDecision ?? new DefaultReconnectDecision();
        _strategy = options.ReconnectStrategy ?? new BackoffReconnectStrategy(options.Backoff, decision);
        _lifecycle = options.Lifecycle ?? new DefaultConnectionLifecycle(_sessionStore);

        _messages = Channel.CreateBounded<MqttPublishPacket>(new BoundedChannelOptions(options.Raw.InboundMessageCapacity)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    /// <summary>The current connection state.</summary>
    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    /// <summary>Raised on every state transition.</summary>
    public event Action<ConnectionStateChanged>? StateChanged;

    /// <summary>
    /// Received application messages across all sessions, in arrival order. Completes only when
    /// the client is disposed.
    /// </summary>
    public ChannelReader<MqttPublishPacket> Messages => _messages.Reader;

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
            _subscriptions.AddRange(stored);
        }

        _attempt = 0;
        _lifetime?.Dispose();
        var lifetime = new CancellationTokenSource();
        _lifetime = lifetime;
        _supervisor = Task.Run(() => SuperviseAsync(lifetime.Token), CancellationToken.None);
    }

    /// <summary>
    /// Publishes through the live connection when available; otherwise queues (QoS &gt; 0, or QoS 0
    /// when configured) or drops QoS 0 — always explicitly, never silently.
    /// </summary>
    public async Task<PublishOutcome> PublishAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_raw is { } raw)
        {
            try
            {
                var reason = await raw.PublishAsync(packet, cancellationToken).ConfigureAwait(false);
                return new PublishOutcome(PublishDisposition.Delivered, reason);
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

        IReadOnlyList<MqttTopicFilter> snapshot;
        lock (_subscriptionGate)
        {
            foreach (var filter in topicFilters)
            {
                _subscriptions.RemoveAll(existing => existing.Topic == filter.Topic);
                _subscriptions.Add(filter);
            }

            snapshot = _subscriptions.ToArray();
        }

        await _sessionStore.SaveSubscriptionsAsync(snapshot, cancellationToken).ConfigureAwait(false);

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

        IReadOnlyList<MqttTopicFilter> snapshot;
        lock (_subscriptionGate)
        {
            foreach (var topic in topicFilters)
            {
                _subscriptions.RemoveAll(existing => existing.Topic == topic);
            }

            snapshot = _subscriptions.ToArray();
        }

        await _sessionStore.SaveSubscriptionsAsync(snapshot, cancellationToken).ConfigureAwait(false);

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
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _messages.Writer.TryComplete();
        _lifetime?.Dispose();
    }

    private async Task SuperviseAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Transition(_attempt == 0 ? ConnectionState.Connecting : ConnectionState.Reconnecting);

                RawMqttClient? raw = null;
                MqttConnAckPacket? connAck = null;

                try
                {
                    await _strategy.RunAsync(
                        async token =>
                        {
                            var candidate = new RawMqttClient(_transportFactory, _options.Raw, _time);
                            try
                            {
                                var ack = await candidate.ConnectAsync(_options.Connect, token).ConfigureAwait(false);
                                if (ack.ReasonCode != MqttReasonCode.Success)
                                {
                                    throw new MqttConnectRejectedException(ack.ReasonCode);
                                }

                                raw = candidate;
                                connAck = ack;
                            }
                            catch
                            {
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

                await ForwardSessionMessagesAsync(raw!, cancellationToken).ConfigureAwait(false);

                _raw = null;
                await raw!.DisposeAsync().ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                _attempt++;
                try
                {
                    await _lifecycle.OnConnectionDownAsync(null, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            Transition(ConnectionState.Stopped);
        }
        catch (Exception error)
        {
            Fault(error);
        }
    }

    private async Task ForwardSessionMessagesAsync(RawMqttClient raw, CancellationToken cancellationToken)
    {
        try
        {
            while (await raw.Messages.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (raw.Messages.TryRead(out var message))
                {
                    await _messages.Writer.WriteAsync(message, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown; the supervisor loop observes the token.
        }
        catch (ChannelClosedException)
        {
            // The session faulted; the supervisor reconnects.
        }
    }

    private async Task FlushQueuedAsync(RawMqttClient raw, CancellationToken cancellationToken)
    {
        while (await _messageStore.PeekAsync(cancellationToken).ConfigureAwait(false) is { } queued)
        {
            await raw.PublishAsync(queued, cancellationToken).ConfigureAwait(false);
            await _messageStore.RemoveHeadAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void Fault(Exception error)
    {
        var reason = error as MqttConnectRejectedException ?? error.InnerException as MqttConnectRejectedException;
        Transition(ConnectionState.Faulted, reason?.ReasonCode);
    }

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
            if (attempt > 1)
            {
                client.Transition(client._attempt == 0 ? ConnectionState.Connecting : ConnectionState.Reconnecting);
            }
        }

        public void OnAttemptFailed(int attempt, Exception error)
        {
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
