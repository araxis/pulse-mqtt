using System.Threading.Channels;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Testing;

/// <summary>
/// An in-process MQTT 5 broker for tests: hand it to a client as its transport factory and the
/// whole stack — handshake, keep-alive, QoS acknowledgements, subscriptions, and topic routing
/// between connected clients — works in memory with no external process and no setup.
/// </summary>
/// <remarks>
/// Scope: MQTT 5 clients, no retained messages, no session persistence; messages forward at
/// most at QoS 1. Built for fast deterministic tests, not broker conformance.
/// </remarks>
public sealed class PulseMqttTestBroker : IMqttTransportFactory, IAsyncDisposable
{
    private readonly List<BrokerSession> _sessions = [];
    private readonly object _gate = new();
    private readonly Channel<MqttPublishPacket> _clientPublishes = Channel.CreateUnbounded<MqttPublishPacket>();
    private readonly CancellationTokenSource _lifetime = new();
    private int _packetId;
    private volatile bool _disposed;

    /// <summary>Every message a client published, in arrival order, for assertions.</summary>
    public ChannelReader<MqttPublishPacket> ClientPublishes => _clientPublishes.Reader;

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
    public ValueTask PublishAsync(MqttPublishPacket message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return RouteAsync(message, cancellationToken);
    }

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

    private async ValueTask RouteAsync(MqttPublishPacket message, CancellationToken cancellationToken)
    {
        BrokerSession[] sessions;
        lock (_gate)
        {
            sessions = [.. _sessions];
        }

        foreach (var session in sessions)
        {
            var maximum = session.MatchSubscription(message.Topic);
            if (maximum is null)
            {
                continue;
            }

            var qos = (MqttQualityOfService)Math.Min(
                Math.Min((byte)message.QualityOfService, (byte)maximum.Value),
                (byte)MqttQualityOfService.AtLeastOnce);

            var forward = message with
            {
                QualityOfService = qos,
                PacketIdentifier = qos > MqttQualityOfService.AtMostOnce ? NextPacketId() : null,
                Dup = false,
                Retain = false,
            };
            await session.SendAsync(forward, cancellationToken).ConfigureAwait(false);
        }
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

    private sealed class BrokerSession : IAsyncDisposable
    {
        private readonly PulseMqttTestBroker _broker;
        private readonly MqttConnection _connection;
        private readonly Dictionary<string, MqttTopicFilter> _subscriptions = [];
        private readonly object _subscriptionGate = new();
        private Task? _loop;

        public BrokerSession(PulseMqttTestBroker broker, IMqttTransport transport)
        {
            _broker = broker;
            _connection = new MqttConnection(transport, new MqttConnectionOptions());
        }

        public void Start(CancellationToken cancellationToken)
        {
            _connection.Start();
            _loop = Task.Run(() => ServeAsync(cancellationToken), CancellationToken.None);
        }

        public MqttQualityOfService? MatchSubscription(string topic)
        {
            lock (_subscriptionGate)
            {
                MqttQualityOfService? best = null;
                foreach (var filter in _subscriptions.Values)
                {
                    if (MqttTopicFilterMatcher.Matches(filter.Topic, topic)
                        && (best is null || filter.MaximumQualityOfService > best))
                    {
                        best = filter.MaximumQualityOfService;
                    }
                }

                return best;
            }
        }

        public ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken) =>
            _connection.SendAsync(packet, cancellationToken);

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
            }
        }

        private async ValueTask<bool> HandleAsync(MqttPacket packet, CancellationToken cancellationToken)
        {
            switch (packet)
            {
                case MqttConnectPacket connect:
                    await SendAsync(new MqttConnAckPacket { ProtocolVersion = connect.ProtocolVersion }, cancellationToken)
                        .ConfigureAwait(false);
                    break;

                case MqttPingReqPacket:
                    await SendAsync(new MqttPingRespPacket(), cancellationToken).ConfigureAwait(false);
                    break;

                case MqttSubscribePacket subscribe:
                    lock (_subscriptionGate)
                    {
                        foreach (var filter in subscribe.TopicFilters)
                        {
                            _subscriptions[filter.Topic] = filter;
                        }
                    }

                    await SendAsync(
                        new MqttSubAckPacket
                        {
                            PacketIdentifier = subscribe.PacketIdentifier,
                            ReasonCodes = [.. subscribe.TopicFilters.Select(f => (MqttReasonCode)f.MaximumQualityOfService)],
                        },
                        cancellationToken).ConfigureAwait(false);
                    break;

                case MqttUnsubscribePacket unsubscribe:
                    lock (_subscriptionGate)
                    {
                        foreach (var topic in unsubscribe.TopicFilters)
                        {
                            _subscriptions.Remove(topic);
                        }
                    }

                    await SendAsync(
                        new MqttUnsubAckPacket
                        {
                            PacketIdentifier = unsubscribe.PacketIdentifier,
                            ReasonCodes = [.. unsubscribe.TopicFilters.Select(_ => MqttReasonCode.Success)],
                        },
                        cancellationToken).ConfigureAwait(false);
                    break;

                case MqttPublishPacket publish:
                    _broker.RecordClientPublish(publish);
                    switch (publish.QualityOfService)
                    {
                        case MqttQualityOfService.AtLeastOnce:
                            await SendAsync(
                                new MqttPublishAckPacket
                                {
                                    PacketType = Codec.MqttPacketType.PubAck,
                                    PacketIdentifier = publish.PacketIdentifier!.Value,
                                },
                                cancellationToken).ConfigureAwait(false);
                            break;
                        case MqttQualityOfService.ExactlyOnce:
                            await SendAsync(
                                new MqttPublishAckPacket
                                {
                                    PacketType = Codec.MqttPacketType.PubRec,
                                    PacketIdentifier = publish.PacketIdentifier!.Value,
                                },
                                cancellationToken).ConfigureAwait(false);
                            break;
                    }

                    await _broker.RouteAsync(publish, cancellationToken).ConfigureAwait(false);
                    break;

                case MqttPublishAckPacket { PacketType: Codec.MqttPacketType.PubRel } release:
                    await SendAsync(
                        new MqttPublishAckPacket
                        {
                            PacketType = Codec.MqttPacketType.PubComp,
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
    }
}
