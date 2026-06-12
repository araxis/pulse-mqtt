using System.Collections.Concurrent;
using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Connection;

/// <summary>
/// A non-resilient MQTT client: one connection, the CONNECT/CONNACK handshake, protocol keep-alive,
/// QoS 0/1/2 publishing, and subscriptions. Received application messages surface on
/// <see cref="Messages"/>. Reconnection, re-subscription, and offline queueing live in the
/// resilient layer built on top of this type.
/// </summary>
public sealed class RawMqttClient : IAsyncDisposable
{
    private readonly IMqttTransportFactory _transportFactory;
    private readonly RawMqttClientOptions _options;
    private readonly TimeProvider _time;
    private readonly Channel<MqttPublishPacket> _messages;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ConcurrentDictionary<ushort, TaskCompletionSource<MqttPacket>> _pending = new();
    private readonly MqttPacketIdAllocator _packetIds = new();
    private readonly HashSet<ushort> _inboundQos2 = []; // touched only by the single-threaded pump

    private MqttConnection? _connection;
    private MqttProtocolVersion _protocolVersion = MqttProtocolVersion.V500;
    private Task? _pump;
    private Task? _keepAliveLoop;
    private long _lastSendTimestamp;
    private volatile TaskCompletionSource? _pongSignal;
    private volatile bool _disposed;

    /// <summary>Creates a client that connects through <paramref name="transportFactory"/>.</summary>
    public RawMqttClient(
        IMqttTransportFactory transportFactory,
        RawMqttClientOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(transportFactory);

        _transportFactory = transportFactory;
        _options = options ?? new RawMqttClientOptions();
        _time = timeProvider ?? TimeProvider.System;
        _messages = Channel.CreateBounded<MqttPublishPacket>(new BoundedChannelOptions(_options.InboundMessageCapacity)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    /// <summary>
    /// Received application messages in arrival order. Completes when the connection closes;
    /// completes with an error when the connection faults (protocol violation, keep-alive timeout).
    /// </summary>
    public ChannelReader<MqttPublishPacket> Messages => _messages.Reader;

    /// <summary>
    /// An optional delivery sink that replaces <see cref="Messages"/>: when set before
    /// <see cref="ConnectAsync"/>, inbound application messages go straight to it from the inbound
    /// pump and the <see cref="Messages"/> channel stays empty — one less queue between the wire
    /// and a consumer that already has its own. The connection's close no longer completes
    /// anything; observe <see cref="Completion"/> instead.
    /// </summary>
    public Func<MqttPublishPacket, CancellationToken, ValueTask>? MessageSink { get; set; }

    /// <summary>Completes when the inbound pump stops — the connection is over, however it ended.</summary>
    public Task Completion => _pump ?? Task.CompletedTask;

    /// <summary>
    /// The broker's DISCONNECT, when the session ended with one. <see langword="null"/> for a
    /// socket-level loss or a client-initiated close.
    /// </summary>
    public MqttServerDisconnectedException? ServerDisconnect { get; private set; }

    /// <summary>
    /// Connects the transport, performs the CONNECT/CONNACK handshake, and starts the inbound pump
    /// and keep-alive. A non-success CONNACK is returned to the caller and the connection is closed.
    /// </summary>
    /// <exception cref="MqttException">The broker did not answer in time or closed during the handshake.</exception>
    /// <exception cref="MqttProtocolException">The broker's first packet was not a CONNACK.</exception>
    public async Task<MqttConnAckPacket> ConnectAsync(MqttConnectPacket connect, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connect);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection is not null)
        {
            throw new InvalidOperationException("The client is already connected.");
        }

        var transport = await _transportFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);
        var connection = new MqttConnection(transport, _options.Connection with { ProtocolVersion = connect.ProtocolVersion });
        connection.Start();

        try
        {
            await SendThroughAsync(connection, connect, cancellationToken).ConfigureAwait(false);

            MqttPacket first;
            try
            {
                first = await connection.Inbound.ReadAsync(cancellationToken).AsTask()
                    .WaitAsync(_options.ConnAckTimeout, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                throw new MqttException("The broker did not answer the CONNECT in time.");
            }
            catch (ChannelClosedException ex)
            {
                throw new MqttException("The connection closed during the handshake.", ex.InnerException ?? ex);
            }

            if (first is not MqttConnAckPacket connAck)
            {
                throw new MqttProtocolException($"Expected a CONNACK but received {first.GetType().Name}.");
            }

            if (connAck.ReasonCode != MqttReasonCode.Success)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                return connAck;
            }

            _protocolVersion = connect.ProtocolVersion;
            _connection = connection;

            // Honor the broker's limits from here on. The CONNECT itself is exempt — the limit
            // is only known once the CONNACK arrives.
            connection.MaximumOutboundPacketSize = connAck.MaximumPacketSize;

            // Acknowledgements complete their waiters on the receive loop itself; only packets
            // that need queue semantics or trigger sends flow through the pump.
            connection.InboundFilter = TryHandleInline;
            _pump = Task.Run(() => PumpInboundAsync(connection, _lifetime.Token), CancellationToken.None);

            var keepAliveSeconds = connAck.ServerKeepAlive ?? connect.KeepAliveSeconds;
            if (keepAliveSeconds > 0)
            {
                var interval = TimeSpan.FromSeconds(keepAliveSeconds);
                _keepAliveLoop = Task.Run(() => KeepAliveLoopAsync(connection, interval, _lifetime.Token), CancellationToken.None);
            }

            return connAck;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Publishes a message at its declared QoS and returns the broker's result. The client assigns
    /// the packet identifier; QoS 0 always returns <see cref="MqttReasonCode.Success"/>.
    /// </summary>
    /// <exception cref="MqttException">The broker did not acknowledge in time or the connection closed.</exception>
    public async Task<MqttReasonCode> PublishAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);
        if (packet.PacketIdentifier is not null)
        {
            throw new ArgumentException("The client assigns packet identifiers.", nameof(packet));
        }

        var connection = ConnectedOrThrow();

        if (packet.QualityOfService == MqttQualityOfService.AtMostOnce)
        {
            await SendThroughAsync(connection, packet with { ProtocolVersion = _protocolVersion }, cancellationToken)
                .ConfigureAwait(false);
            return MqttReasonCode.Success;
        }

        var id = _packetIds.Rent();
        try
        {
            packet = packet with { ProtocolVersion = _protocolVersion, PacketIdentifier = id };

            if (packet.QualityOfService == MqttQualityOfService.AtLeastOnce)
            {
                var response = await RequestAsync(connection, packet, id, cancellationToken).ConfigureAwait(false);
                return response is MqttPublishAckPacket { PacketType: MqttPacketType.PubAck } pubAck
                    ? pubAck.ReasonCode
                    : throw new MqttProtocolException($"Expected a PUBACK for {id} but received {response.GetType().Name}.");
            }

            var first = await RequestAsync(connection, packet, id, cancellationToken).ConfigureAwait(false);
            if (first is not MqttPublishAckPacket { PacketType: MqttPacketType.PubRec } pubRec)
            {
                throw new MqttProtocolException($"Expected a PUBREC for {id} but received {first.GetType().Name}.");
            }

            if ((byte)pubRec.ReasonCode >= 0x80)
            {
                return pubRec.ReasonCode;
            }

            var release = new MqttPublishAckPacket
            {
                PacketType = MqttPacketType.PubRel,
                PacketIdentifier = id,
                ProtocolVersion = _protocolVersion,
            };
            var second = await RequestAsync(connection, release, id, cancellationToken).ConfigureAwait(false);
            return second is MqttPublishAckPacket { PacketType: MqttPacketType.PubComp } pubComp
                ? pubComp.ReasonCode
                : throw new MqttProtocolException($"Expected a PUBCOMP for {id} but received {second.GetType().Name}.");
        }
        finally
        {
            _packetIds.Return(id);
        }
    }

    /// <summary>Subscribes to one or more topic filters and returns the broker's per-filter results.</summary>
    public async Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        IReadOnlyList<MqttTopicFilter> topicFilters,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        var connection = ConnectedOrThrow();

        var id = _packetIds.Rent();
        try
        {
            var subscribe = new MqttSubscribePacket
            {
                PacketIdentifier = id,
                TopicFilters = topicFilters,
                ProtocolVersion = _protocolVersion,
            };
            var response = await RequestAsync(connection, subscribe, id, cancellationToken).ConfigureAwait(false);
            return response is MqttSubAckPacket subAck
                ? subAck.ReasonCodes
                : throw new MqttProtocolException($"Expected a SUBACK for {id} but received {response.GetType().Name}.");
        }
        finally
        {
            _packetIds.Return(id);
        }
    }

    /// <summary>Removes one or more subscriptions. The result list is empty for MQTT 3.1.1.</summary>
    public async Task<IReadOnlyList<MqttReasonCode>> UnsubscribeAsync(
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        var connection = ConnectedOrThrow();

        var id = _packetIds.Rent();
        try
        {
            var unsubscribe = new MqttUnsubscribePacket
            {
                PacketIdentifier = id,
                TopicFilters = topicFilters,
                ProtocolVersion = _protocolVersion,
            };
            var response = await RequestAsync(connection, unsubscribe, id, cancellationToken).ConfigureAwait(false);
            return response is MqttUnsubAckPacket unsubAck
                ? unsubAck.ReasonCodes
                : throw new MqttProtocolException($"Expected an UNSUBACK for {id} but received {response.GetType().Name}.");
        }
        finally
        {
            _packetIds.Return(id);
        }
    }

    /// <summary>Sends a DISCONNECT (best effort) and tears the client down.</summary>
    public async Task DisconnectAsync(CancellationToken cancellationToken)
    {
        if (_connection is { } connection && !_disposed)
        {
            try
            {
                await SendThroughAsync(
                    connection,
                    new MqttDisconnectPacket { ProtocolVersion = _protocolVersion },
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is MqttException or ObjectDisposedException)
            {
                // The connection is already gone; proceed with the teardown.
            }
        }

        await DisposeAsync().ConfigureAwait(false);
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

        if (_keepAliveLoop is { } keepAlive)
        {
            await keepAlive.ConfigureAwait(false); // the loop swallows its own shutdown exceptions
        }

        if (_pump is { } pump)
        {
            await pump.ConfigureAwait(false); // the pump completes the channel instead of throwing
        }

        if (_connection is { } connection)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        _messages.Writer.TryComplete();
        FailPending(new MqttException("The client was disposed before the broker acknowledged."));
        _lifetime.Dispose();
    }

    private MqttConnection ConnectedOrThrow()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _connection ?? throw new InvalidOperationException("Connect before performing operations.");
    }

    private async Task<MqttPacket> RequestAsync(
        MqttConnection connection,
        MqttPacket request,
        ushort id,
        CancellationToken cancellationToken)
    {
        var pending = new TaskCompletionSource<MqttPacket>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(id, pending))
        {
            throw new InvalidOperationException($"Packet identifier {id} already has a pending operation.");
        }

        try
        {
            await SendThroughAsync(connection, request, cancellationToken).ConfigureAwait(false);

            try
            {
                return await pending.Task
                    .WaitAsync(_options.AcknowledgementTimeout, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                throw new MqttException($"The broker did not acknowledge packet {id} in time.");
            }
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    private async ValueTask SendThroughAsync(MqttConnection connection, MqttPacket packet, CancellationToken cancellationToken)
    {
        await connection.SendAsync(packet, cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _lastSendTimestamp, _time.GetTimestamp());
    }

    // Runs on the connection's receive loop: completes waiters without a queue hop. PUBREL is
    // excluded because answering it sends a packet, which the receive loop must not do.
    private bool TryHandleInline(MqttPacket packet)
    {
        switch (packet)
        {
            case MqttPublishAckPacket { PacketType: not MqttPacketType.PubRel } ack:
                CompletePending(ack.PacketIdentifier, ack);
                return true;
            case MqttSubAckPacket subAck:
                CompletePending(subAck.PacketIdentifier, subAck);
                return true;
            case MqttUnsubAckPacket unsubAck:
                CompletePending(unsubAck.PacketIdentifier, unsubAck);
                return true;
            case MqttPingRespPacket:
                _pongSignal?.TrySetResult();
                return true;
            default:
                return false;
        }
    }

    private async Task PumpInboundAsync(MqttConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            while (await connection.Inbound.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (connection.Inbound.TryRead(out var packet))
                {
                    switch (packet)
                    {
                        case MqttPublishPacket publish:
                            await HandleInboundPublishAsync(connection, publish, cancellationToken).ConfigureAwait(false);
                            break;
                        case MqttPublishAckPacket { PacketType: MqttPacketType.PubRel } pubRel:
                            _inboundQos2.Remove(pubRel.PacketIdentifier);
                            await SendThroughAsync(
                                connection,
                                new MqttPublishAckPacket
                                {
                                    PacketType = MqttPacketType.PubComp,
                                    PacketIdentifier = pubRel.PacketIdentifier,
                                    ProtocolVersion = _protocolVersion,
                                },
                                cancellationToken).ConfigureAwait(false);
                            break;
                        case MqttDisconnectPacket disconnect:
                            // The broker ended the session on purpose: an orderly close, not a
                            // protocol error. Record the reason and stop — waiters fail with it.
                            var serverDisconnect = new MqttServerDisconnectedException(
                                disconnect.ReasonCode, disconnect.ReasonString, disconnect.ServerReference);
                            ServerDisconnect = serverDisconnect;
                            _messages.Writer.TryComplete(serverDisconnect);
                            FailPending(serverDisconnect);
                            return;
                        default:
                            // Acknowledgements complete inline via the inbound filter;
                            // AUTH handling arrives with the enhanced-authentication feature.
                            break;
                    }
                }
            }

            _messages.Writer.TryComplete();
            FailPending(new MqttException("The connection closed before the broker acknowledged."));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _messages.Writer.TryComplete();
            FailPending(new MqttException("The client shut down before the broker acknowledged."));
        }
        catch (ChannelClosedException ex)
        {
            var error = ex.InnerException ?? ex;
            _messages.Writer.TryComplete(error);
            FailPending(error);
        }
        catch (Exception ex)
        {
            _messages.Writer.TryComplete(ex);
            FailPending(ex);
        }
    }

    private async ValueTask HandleInboundPublishAsync(
        MqttConnection connection,
        MqttPublishPacket publish,
        CancellationToken cancellationToken)
    {
        switch (publish.QualityOfService)
        {
            case MqttQualityOfService.AtMostOnce:
                await DeliverAsync(publish, cancellationToken).ConfigureAwait(false);
                break;

            case MqttQualityOfService.AtLeastOnce:
            {
                var id = RequiredId(publish);
                await DeliverAsync(publish, cancellationToken).ConfigureAwait(false);
                await SendThroughAsync(
                    connection,
                    new MqttPublishAckPacket
                    {
                        PacketType = MqttPacketType.PubAck,
                        PacketIdentifier = id,
                        ProtocolVersion = _protocolVersion,
                    },
                    cancellationToken).ConfigureAwait(false);
                break;
            }

            case MqttQualityOfService.ExactlyOnce:
            {
                var id = RequiredId(publish);
                if (_inboundQos2.Add(id))
                {
                    await DeliverAsync(publish, cancellationToken).ConfigureAwait(false);
                }

                await SendThroughAsync(
                    connection,
                    new MqttPublishAckPacket
                    {
                        PacketType = MqttPacketType.PubRec,
                        PacketIdentifier = id,
                        ProtocolVersion = _protocolVersion,
                    },
                    cancellationToken).ConfigureAwait(false);
                break;
            }
        }

        static ushort RequiredId(MqttPublishPacket publish) =>
            publish.PacketIdentifier
            ?? throw new MqttProtocolException("A QoS > 0 PUBLISH must carry a packet identifier.");
    }

    private ValueTask DeliverAsync(MqttPublishPacket publish, CancellationToken cancellationToken) =>
        MessageSink is { } sink
            ? sink(publish, cancellationToken)
            : _messages.Writer.WriteAsync(publish, cancellationToken);

    private void CompletePending(ushort id, MqttPacket response)
    {
        if (_pending.TryGetValue(id, out var pending))
        {
            pending.TrySetResult(response);
        }
    }

    private void FailPending(Exception error)
    {
        foreach (var entry in _pending)
        {
            entry.Value.TrySetException(error);
        }
    }

    private async Task KeepAliveLoopAsync(MqttConnection connection, TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var idle = interval - _time.GetElapsedTime(Volatile.Read(ref _lastSendTimestamp));
                if (idle > TimeSpan.Zero)
                {
                    await Task.Delay(idle, _time, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var pong = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _pongSignal = pong;
                await SendThroughAsync(connection, new MqttPingReqPacket(), cancellationToken).ConfigureAwait(false);

                try
                {
                    await pong.Task.WaitAsync(_options.PingResponseTimeout, _time, cancellationToken).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    var error = new MqttException("The broker did not answer a PINGREQ within the keep-alive window.");
                    _messages.Writer.TryComplete(error);
                    FailPending(error);
                    await connection.DisposeAsync().ConfigureAwait(false);
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown path.
        }
        catch (Exception ex) when (ex is MqttException or ObjectDisposedException)
        {
            // The connection failed mid-ping; the pump reports the fault to consumers.
        }
    }
}
