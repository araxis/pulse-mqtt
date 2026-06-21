using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Testing;

/// <summary>
/// An in-process MQTT broker for tests: hand it to a client as its transport factory and the
/// whole stack — handshake, keep-alive, QoS acknowledgements, subscriptions, and topic routing
/// between connected clients — works in memory with no external process and no setup.
/// </summary>
/// <remarks>
/// Defaults match the original lightweight harness: no retained storage, no persistent sessions,
/// and forwarded messages capped at QoS 1. Enable options when a test needs more broker realism.
/// Built for fast deterministic tests, not broker conformance.
/// </remarks>
public sealed class PulseMqttTestBroker : IMqttTransportFactory, IAsyncDisposable
{
    private readonly PulseMqttTestBrokerOptions _options;
    private readonly List<BrokerSession> _sessions = [];
    private readonly Dictionary<string, BrokerSessionState> _persistentSessions = [];
    private readonly Dictionary<string, MqttPublishPacket> _retainedMessages = [];
    private readonly object _gate = new();
    private readonly object _retainedGate = new();
    private readonly Channel<MqttPublishPacket> _clientPublishes = Channel.CreateUnbounded<MqttPublishPacket>();
    private readonly CancellationTokenSource _lifetime = new();
    private int _packetId;
    private volatile bool _disposed;

    /// <summary>Creates a broker with the backward-compatible lightweight defaults.</summary>
    public PulseMqttTestBroker()
        : this(new PulseMqttTestBrokerOptions())
    {
    }

    /// <summary>Creates a broker with explicit test behavior options.</summary>
    public PulseMqttTestBroker(PulseMqttTestBrokerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!Enum.IsDefined(options.MaximumForwardQualityOfService))
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.MaximumForwardQualityOfService,
                "Maximum forward QoS must be a defined MQTT QoS value.");
        }

        _options = options;
    }

    /// <summary>Every message a client published, in arrival order, for assertions.</summary>
    public ChannelReader<MqttPublishPacket> ClientPublishes => _clientPublishes.Reader;

    /// <summary>
    /// Compatibility shortcut for the old session-resume toggle. When set, it enables the same
    /// persistent-session behavior as <see cref="PulseMqttTestBrokerOptions.PersistentSessions"/>.
    /// </summary>
    public bool ResumeSessions { get; init; }

    /// <summary>Hands a connecting client its end of a new in-memory session.</summary>
    public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var session = new BrokerSession(this, serverTransport);
        lock (_gate)
        {
            _sessions.Add(session);
        }

        session.Start(_lifetime.Token);
        return ValueTask.FromResult(clientTransport);
    }

    /// <summary>Publishes a message from the broker side to every matching subscriber.</summary>
    public async ValueTask PublishAsync(MqttPublishPacket message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        StoreRetainedMessage(message);
        await RouteAsync(message, origin: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends an optional broker DISCONNECT to sessions with the supplied client identifier, then
    /// closes those sessions. Returns the number of connected sessions selected.
    /// </summary>
    public ValueTask<int> DisconnectClientAsync(
        string clientId,
        MqttDisconnectPacket? packet = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        return DisconnectSessionsAsync(
            session => string.Equals(session.ClientId, clientId, StringComparison.Ordinal),
            packet,
            cancellationToken);
    }

    /// <summary>
    /// Sends an optional broker DISCONNECT to every connected session, then closes them. Returns
    /// the number of connected sessions selected.
    /// </summary>
    public ValueTask<int> DisconnectAllAsync(
        MqttDisconnectPacket? packet = null,
        CancellationToken cancellationToken = default) =>
        DisconnectSessionsAsync(_ => true, packet, cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _lifetime.CancelAsync().ConfigureAwait(false);

        BrokerSession[] sessions;
        lock (_gate)
        {
            sessions = [.. _sessions];
            _sessions.Clear();
        }

        foreach (var session in sessions)
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }

        _clientPublishes.Writer.TryComplete();
        _lifetime.Dispose();
    }

    private bool PersistentSessionsEnabled => _options.PersistentSessions || ResumeSessions;

    private async ValueTask<int> DisconnectSessionsAsync(
        Func<BrokerSession, bool> predicate,
        MqttDisconnectPacket? packet,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        BrokerSession[] sessions;
        lock (_gate)
        {
            sessions = [.. _sessions.Where(predicate)];
        }

        foreach (var session in sessions)
        {
            await session.DisconnectAsync(packet, cancellationToken).ConfigureAwait(false);
        }

        return sessions.Length;
    }

    private async ValueTask RouteAsync(
        MqttPublishPacket message,
        BrokerSession? origin,
        CancellationToken cancellationToken)
    {
        BrokerSession[] sessions;
        lock (_gate)
        {
            sessions = [.. _sessions];
        }

        foreach (var session in sessions)
        {
            var filter = session.MatchSubscription(message.Topic, origin);
            if (filter is null)
            {
                continue;
            }

            await ForwardAsync(
                    session,
                    message,
                    filter,
                    retainedReplay: false,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async ValueTask ReplayRetainedAsync(
        BrokerSession session,
        MqttTopicFilter filter,
        CancellationToken cancellationToken)
    {
        if (!_options.RetainedMessages)
        {
            return;
        }

        MqttPublishPacket[] retained;
        lock (_retainedGate)
        {
            retained = [.. _retainedMessages.Values.Where(message => MqttTopicFilterMatcher.Matches(filter.Topic, message.Topic))];
        }

        foreach (var message in retained)
        {
            await ForwardAsync(session, message, filter, retainedReplay: true, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ForwardAsync(
        BrokerSession session,
        MqttPublishPacket message,
        MqttTopicFilter filter,
        bool retainedReplay,
        CancellationToken cancellationToken)
    {
        var qos = (MqttQualityOfService)Math.Min(
            Math.Min((byte)message.QualityOfService, (byte)filter.MaximumQualityOfService),
            (byte)_options.MaximumForwardQualityOfService);

        var forward = message with
        {
            ProtocolVersion = session.ProtocolVersion,
            QualityOfService = qos,
            PacketIdentifier = qos > MqttQualityOfService.AtMostOnce ? NextPacketId() : null,
            Dup = false,
            Retain = retainedReplay || (filter.RetainAsPublished && message.Retain),
        };

        await session.SendAsync(forward, cancellationToken).ConfigureAwait(false);
    }

    private ushort NextPacketId()
    {
        var next = (ushort)(Interlocked.Increment(ref _packetId) & 0xFFFF);
        return next == 0 ? NextPacketId() : next;
    }

    private void Remove(BrokerSession session)
    {
        lock (_gate)
        {
            _sessions.Remove(session);
        }
    }

    private void RecordClientPublish(MqttPublishPacket message) => _clientPublishes.Writer.TryWrite(message);

    private void StoreRetainedMessage(MqttPublishPacket message)
    {
        if (!_options.RetainedMessages || !message.Retain)
        {
            return;
        }

        lock (_retainedGate)
        {
            if (message.Payload.IsEmpty)
            {
                _retainedMessages.Remove(message.Topic);
                return;
            }

            _retainedMessages[message.Topic] = message with
            {
                PacketIdentifier = null,
                Dup = false,
            };
        }
    }

    private BrokerSessionState OpenSession(MqttConnectPacket connect, out bool sessionPresent)
    {
        sessionPresent = false;
        if (!PersistentSessionsEnabled || string.IsNullOrEmpty(connect.ClientId))
        {
            return new BrokerSessionState();
        }

        lock (_gate)
        {
            if (connect.CleanStart || connect.SessionExpiryInterval == 0)
            {
                _persistentSessions.Remove(connect.ClientId);
                return new BrokerSessionState();
            }

            if (!_persistentSessions.TryGetValue(connect.ClientId, out var state))
            {
                state = new BrokerSessionState();
                _persistentSessions[connect.ClientId] = state;
                return state;
            }

            sessionPresent = true;
            return state;
        }
    }

    private bool HasExistingSession(MqttConnectPacket connect)
    {
        if (!PersistentSessionsEnabled
            || string.IsNullOrEmpty(connect.ClientId)
            || connect.CleanStart
            || connect.SessionExpiryInterval == 0)
        {
            return false;
        }

        lock (_gate)
        {
            return _persistentSessions.ContainsKey(connect.ClientId);
        }
    }

    private sealed class BrokerSessionState
    {
        public object Gate { get; } = new();

        public Dictionary<string, MqttTopicFilter> Subscriptions { get; } = [];
    }

    private sealed class BrokerSession : IAsyncDisposable
    {
        private readonly PulseMqttTestBroker _broker;
        private readonly MqttConnection _connection;
        private BrokerSessionState _state = new();
        private Task? _loop;

        public BrokerSession(PulseMqttTestBroker broker, IMqttTransport transport)
        {
            _broker = broker;
            _connection = new MqttConnection(transport, new MqttConnectionOptions());
        }

        public MqttProtocolVersion ProtocolVersion { get; private set; } = MqttProtocolVersion.V500;

        public string? ClientId { get; private set; }

        public void Start(CancellationToken cancellationToken)
        {
            _connection.Start();
            _loop = Task.Run(() => ServeAsync(cancellationToken), CancellationToken.None);
        }

        public MqttTopicFilter? MatchSubscription(string topic, BrokerSession? origin)
        {
            lock (_state.Gate)
            {
                MqttTopicFilter? best = null;
                foreach (var filter in _state.Subscriptions.Values)
                {
                    if (this == origin && filter.NoLocal)
                    {
                        continue;
                    }

                    if (MqttTopicFilterMatcher.Matches(filter.Topic, topic)
                        && (best is null || filter.MaximumQualityOfService > best.MaximumQualityOfService))
                    {
                        best = filter;
                    }
                }

                return best;
            }
        }

        public ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken) =>
            _connection.SendAsync(packet, cancellationToken);

        public async ValueTask DisconnectAsync(MqttDisconnectPacket? packet, CancellationToken cancellationToken)
        {
            try
            {
                if (packet is not null)
                {
                    await SendAsync(packet with { ProtocolVersion = ProtocolVersion }, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // The peer may already be closing; the helper still tears down the session.
            }

            await DisposeAsync().ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
            if (_loop is { } loop)
            {
                try
                {
                    await loop.ConfigureAwait(false);
                }
                catch
                {
                    // Session teardown.
                }
            }
        }

        private async Task ServeAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (await _connection.Inbound.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (_connection.Inbound.TryRead(out var packet))
                    {
                        if (!await HandleAsync(packet, cancellationToken).ConfigureAwait(false))
                        {
                            return;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Broker shutdown.
            }
            catch (ChannelClosedException)
            {
                // The client closed abruptly.
            }
            finally
            {
                _broker.Remove(this);

                // Close the broker's half so the peer's transport observes the disconnect. A real socket
                // closes both directions; the in-memory pipe must be completed explicitly, otherwise a
                // client whose transport is dropped never sees the connection go down.
                await _connection.DisposeAsync().ConfigureAwait(false);
            }
        }

        private async ValueTask<bool> HandleAsync(MqttPacket packet, CancellationToken cancellationToken)
        {
            switch (packet)
            {
                case MqttConnectPacket connect:
                {
                    ProtocolVersion = connect.ProtocolVersion;
                    var sessionPresent = _broker.HasExistingSession(connect);
                    var defaultConnAck = new MqttConnAckPacket
                    {
                        ProtocolVersion = ProtocolVersion,
                        SessionPresent = sessionPresent,
                    };
                    var connAck = _broker._options.ConnAckFactory is { } factory
                        ? factory(new PulseMqttTestBrokerConnectContext(
                            connect.ClientId,
                            ProtocolVersion,
                            connect,
                            sessionPresent,
                            defaultConnAck))
                        : defaultConnAck;

                    connAck = connAck with { ProtocolVersion = ProtocolVersion };
                    if (connAck.ReasonCode == MqttReasonCode.Success)
                    {
                        _state = _broker.OpenSession(connect, out _);
                        ClientId = connect.ClientId;
                    }

                    await SendAsync(connAck, cancellationToken).ConfigureAwait(false);
                    if (connAck.ReasonCode != MqttReasonCode.Success)
                    {
                        return false;
                    }

                    break;
                }

                case MqttPingReqPacket:
                    await SendAsync(new MqttPingRespPacket(), cancellationToken).ConfigureAwait(false);
                    break;

                case MqttSubscribePacket subscribe:
                {
                    var defaultSubAck = new MqttSubAckPacket
                    {
                        ProtocolVersion = ProtocolVersion,
                        PacketIdentifier = subscribe.PacketIdentifier,
                        ReasonCodes = [.. subscribe.TopicFilters.Select(f => (MqttReasonCode)f.MaximumQualityOfService)],
                    };
                    var subAck = _broker._options.SubAckFactory is { } factory
                        ? factory(new PulseMqttTestBrokerSubscribeContext(
                            ClientId ?? string.Empty,
                            ProtocolVersion,
                            subscribe,
                            defaultSubAck))
                        : defaultSubAck;

                    subAck = subAck with
                    {
                        ProtocolVersion = ProtocolVersion,
                        PacketIdentifier = subscribe.PacketIdentifier,
                    };

                    if (subAck.ReasonCodes.Count != subscribe.TopicFilters.Count)
                    {
                        throw new InvalidOperationException(
                            "SubAckFactory must return exactly one reason code per topic filter.");
                    }

                    List<MqttTopicFilter> retainedFilters = [];
                    lock (_state.Gate)
                    {
                        for (var i = 0; i < subscribe.TopicFilters.Count; i++)
                        {
                            var filter = subscribe.TopicFilters[i];
                            if (!IsGrantedSubscription(subAck.ReasonCodes[i]))
                            {
                                continue;
                            }

                            var existed = _state.Subscriptions.ContainsKey(filter.Topic);
                            _state.Subscriptions[filter.Topic] = filter;
                            if (ShouldReplayRetained(filter, existed))
                            {
                                retainedFilters.Add(filter);
                            }
                        }
                    }

                    await SendAsync(subAck, cancellationToken).ConfigureAwait(false);

                    foreach (var filter in retainedFilters)
                    {
                        await _broker.ReplayRetainedAsync(this, filter, cancellationToken).ConfigureAwait(false);
                    }

                    break;
                }

                case MqttUnsubscribePacket unsubscribe:
                    lock (_state.Gate)
                    {
                        foreach (var topic in unsubscribe.TopicFilters)
                        {
                            _state.Subscriptions.Remove(topic);
                        }
                    }

                    await SendAsync(
                        new MqttUnsubAckPacket
                        {
                            ProtocolVersion = ProtocolVersion,
                            PacketIdentifier = unsubscribe.PacketIdentifier,
                            ReasonCodes = [.. unsubscribe.TopicFilters.Select(_ => MqttReasonCode.Success)],
                        },
                        cancellationToken).ConfigureAwait(false);
                    break;

                case MqttPublishPacket publish:
                    _broker.RecordClientPublish(publish);
                    if (CreateDefaultPublishAcknowledgement(publish) is { } defaultAcknowledgement)
                    {
                        var acknowledgement = ApplyPublishAckFactory(publish, defaultAcknowledgement);
                        if (acknowledgement is null)
                        {
                            break;
                        }

                        await SendAsync(acknowledgement, cancellationToken).ConfigureAwait(false);
                        if (!IsSuccessfulAcknowledgement(acknowledgement.ReasonCode))
                        {
                            break;
                        }
                    }

                    _broker.StoreRetainedMessage(publish);
                    await _broker.RouteAsync(publish, origin: this, cancellationToken).ConfigureAwait(false);
                    break;

                case MqttPublishAckPacket { PacketType: MqttPacketType.PubRec } pubRec:
                    await SendAsync(
                        new MqttPublishAckPacket
                        {
                            ProtocolVersion = ProtocolVersion,
                            PacketType = MqttPacketType.PubRel,
                            PacketIdentifier = pubRec.PacketIdentifier,
                        },
                        cancellationToken).ConfigureAwait(false);
                    break;

                case MqttPublishAckPacket { PacketType: MqttPacketType.PubRel } release:
                    await SendAsync(
                        new MqttPublishAckPacket
                        {
                            ProtocolVersion = ProtocolVersion,
                            PacketType = MqttPacketType.PubComp,
                            PacketIdentifier = release.PacketIdentifier,
                        },
                        cancellationToken).ConfigureAwait(false);
                    break;

                case MqttPublishAckPacket:
                    break; // acknowledgements for forwarded messages

                case MqttDisconnectPacket:
                    return false;
            }

            return true;
        }

        private MqttPublishAckPacket? CreateDefaultPublishAcknowledgement(MqttPublishPacket publish) =>
            publish.QualityOfService switch
            {
                MqttQualityOfService.AtLeastOnce => new MqttPublishAckPacket
                {
                    ProtocolVersion = ProtocolVersion,
                    PacketType = MqttPacketType.PubAck,
                    PacketIdentifier = publish.PacketIdentifier!.Value,
                },
                MqttQualityOfService.ExactlyOnce => new MqttPublishAckPacket
                {
                    ProtocolVersion = ProtocolVersion,
                    PacketType = MqttPacketType.PubRec,
                    PacketIdentifier = publish.PacketIdentifier!.Value,
                },
                _ => null,
            };

        private MqttPublishAckPacket? ApplyPublishAckFactory(
            MqttPublishPacket publish,
            MqttPublishAckPacket defaultAcknowledgement)
        {
            var acknowledgement = _broker._options.PublishAckFactory is { } factory
                ? factory(new PulseMqttTestBrokerPublishContext(
                    ClientId ?? string.Empty,
                    ProtocolVersion,
                    publish,
                    defaultAcknowledgement))
                : defaultAcknowledgement;

            return acknowledgement is null
                ? null
                : acknowledgement with
                {
                    ProtocolVersion = ProtocolVersion,
                    PacketType = defaultAcknowledgement.PacketType,
                    PacketIdentifier = defaultAcknowledgement.PacketIdentifier,
                };
        }

        private static bool IsGrantedSubscription(MqttReasonCode reasonCode) =>
            reasonCode is MqttReasonCode.Success
                or MqttReasonCode.GrantedQualityOfService1
                or MqttReasonCode.GrantedQualityOfService2;

        private static bool IsSuccessfulAcknowledgement(MqttReasonCode reasonCode) => (byte)reasonCode < 0x80;

        private bool ShouldReplayRetained(MqttTopicFilter filter, bool subscriptionExisted)
        {
            if (!_broker._options.RetainedMessages)
            {
                return false;
            }

            return filter.RetainHandling switch
            {
                MqttRetainHandling.SendAtSubscribe => true,
                MqttRetainHandling.SendAtSubscribeIfNewSubscription => !subscriptionExisted,
                MqttRetainHandling.DoNotSendAtSubscribe => false,
                _ => false,
            };
        }
    }
}
