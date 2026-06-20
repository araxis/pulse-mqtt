using System.Collections.Concurrent;
using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
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
    private readonly object _inboundQos2Gate = new();
    private readonly HashSet<ushort> _inboundQos2 = [];
    private readonly HashSet<ushort> _pendingAcknowledgedInboundQos2 = [];

    private MqttConnection? _connection;
    private SemaphoreSlim? _sendQuota;
    private string?[]? _inboundAliasTopics;   // index = alias - 1; touched only by the pump
    private Dictionary<string, ushort>? _outboundAliases;   // touched only inside the send lock
    private ushort _outboundAliasMaximum;
    private TaskCompletionSource? _reauthSignal;   // swapped with Interlocked; completed by the pump
    private MqttInFlightSession? _session;
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
    /// connecting, inbound application messages go straight to it from the inbound
    /// pump and the <see cref="Messages"/> channel stays empty — one less queue between the wire
    /// and a consumer that already has its own. The connection's close no longer completes
    /// anything; observe <see cref="Completion"/> instead.
    /// </summary>
    public Func<MqttPublishPacket, CancellationToken, ValueTask>? MessageSink { get; set; }

    /// <summary>
    /// An optional delivery sink that receives inbound application messages with an explicit
    /// acknowledgement context. When set before connecting, QoS 1/2 inbound publishes are not
    /// protocol-acknowledged until the context is completed by the sink or a downstream consumer.
    /// </summary>
    public Func<MqttInboundPublishContext, CancellationToken, ValueTask>? AcknowledgedMessageSink { get; set; }

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
    public Task<MqttConnAckPacket> ConnectAsync(MqttConnectPacket connect, CancellationToken cancellationToken) =>
        ConnectAsync(connect, session: null, cancellationToken);

    /// <summary>
    /// Connects with persistent-session tracking: unfinished QoS exchanges record into
    /// <paramref name="session"/>, and when the broker reports a present session its inbound
    /// duplicate-suppression identifiers are restored and the outbound identifiers reserved so
    /// <see cref="RedeliverAsync"/> can resume them. When the broker did not preserve the
    /// session, the tracked state is discarded.
    /// </summary>
    public async Task<MqttConnAckPacket> ConnectAsync(
        MqttConnectPacket connect,
        MqttInFlightSession? session,
        CancellationToken cancellationToken)
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
            if (_options.Authenticator is { } initialAuthenticator)
            {
                connect = connect with
                {
                    AuthenticationMethod = initialAuthenticator.Method,
                    AuthenticationData = await initialAuthenticator.NextDataAsync(null, cancellationToken).ConfigureAwait(false),
                };
            }

            await SendThroughAsync(connection, connect, cancellationToken).ConfigureAwait(false);

            MqttPacket first;
            while (true)
            {
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

                // Enhanced authentication: answer each challenge until the broker concludes
                // with a CONNACK (success or rejection).
                if (first is MqttAuthPacket { ReasonCode: MqttReasonCode.ContinueAuthentication } challenge)
                {
                    if (_options.Authenticator is not { } authenticator)
                    {
                        throw new MqttProtocolException("The broker started an authentication exchange, but no authenticator is configured.");
                    }

                    var data = await authenticator.NextDataAsync(challenge.AuthenticationData, cancellationToken).ConfigureAwait(false);
                    await SendThroughAsync(
                        connection,
                        new MqttAuthPacket
                        {
                            ReasonCode = MqttReasonCode.ContinueAuthentication,
                            AuthenticationMethod = authenticator.Method,
                            AuthenticationData = data,
                        },
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                break;
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

            // Persistent-session state: restore it while the pump is not yet running (no
            // races), or discard it when the broker reports a fresh session.
            _session = session;
            if (session is not null)
            {
                if (connAck.SessionPresent)
                {
                    var state = session.Snapshot();
                    foreach (var id in state.InboundExactlyOnce)
                    {
                        RestoreAcceptedInboundQos2(id);
                    }

                    foreach (var entry in state.Outbound)
                    {
                        _packetIds.Reserve(entry.Packet.PacketIdentifier!.Value);
                    }
                }
                else
                {
                    await session.ClearAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            // Honor the broker's limits from here on. The CONNECT itself is exempt — the limit
            // is only known once the CONNACK arrives.
            connection.MaximumOutboundPacketSize = connAck.MaximumPacketSize;

            // The broker's receive maximum caps concurrent unacknowledged QoS > 0 publishes.
            // Per the specification the quota frees on PUBACK (QoS 1) or PUBREC (QoS 2).
            if (connAck.ReceiveMaximum is { } receiveMaximum and > 0)
            {
                _sendQuota = new SemaphoreSlim(receiveMaximum, receiveMaximum);
            }

            // Topic aliases, both directions, reset with every connection. Inbound resolution
            // is mandatory the moment this client advertised a maximum; outbound assignment is
            // opt-in and bounded by the broker's advertised maximum.
            if (connect.TopicAliasMaximum is { } inboundAliasMaximum and > 0)
            {
                _inboundAliasTopics = new string?[inboundAliasMaximum];
            }

            if (_options.UseOutboundTopicAliases && connAck.TopicAliasMaximum is { } outboundAliasMaximum and > 0)
            {
                _outboundAliases = new Dictionary<string, ushort>();
                _outboundAliasMaximum = outboundAliasMaximum;
                connection.OutboundTransform = ApplyOutboundTopicAlias;
            }

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

        // The identifier returns to the allocator only when the exchange truly finishes (or the
        // packet never reached the wire). When a persistent session keeps an interrupted entry,
        // the id stays reserved so a still-live connection can never re-rent it and clobber that
        // entry (which would lose the message and collide identifiers on the wire). Without a
        // session there is nothing to protect, so the id always returns.
        var settled = false;

        // The receive-maximum send quota is held for the WHOLE exchange: PUBLISH → PUBACK for
        // QoS 1, and PUBLISH → PUBCOMP for QoS 2 (the final acknowledgement, per MQTT 5 §4.9) — not
        // released early at PUBREC, which would let a burst exceed the broker's receive maximum and
        // a strict broker disconnect the client.
        var quota = _sendQuota;
        var quotaHeld = false;
        try
        {
            if (quota is not null)
            {
                await quota.WaitScopedAsync(cancellationToken).ConfigureAwait(false);
                quotaHeld = true;
            }

            packet = packet with { ProtocolVersion = _protocolVersion, PacketIdentifier = id };

            if (packet.QualityOfService == MqttQualityOfService.AtLeastOnce)
            {
                var response = await ExchangeAsync(
                    connection, packet, id, MqttInFlightStage.AwaitingPubAck, cancellationToken).ConfigureAwait(false);

                if (response is not MqttPublishAckPacket { PacketType: MqttPacketType.PubAck } pubAck)
                {
                    throw new MqttProtocolException($"Expected a PUBACK for {id} but received {response.GetType().Name}.");
                }

                if (_session is { } qos1Session)
                {
                    await qos1Session.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
                }

                settled = true;
                return pubAck.ReasonCode;
            }

            var first = await ExchangeAsync(
                connection, packet, id, MqttInFlightStage.AwaitingPubRec, cancellationToken).ConfigureAwait(false);

            if (first is not MqttPublishAckPacket { PacketType: MqttPacketType.PubRec } pubRec)
            {
                throw new MqttProtocolException($"Expected a PUBREC for {id} but received {first.GetType().Name}.");
            }

            if ((byte)pubRec.ReasonCode >= 0x80)
            {
                if (_session is { } rejectedSession)
                {
                    await rejectedSession.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
                }

                settled = true;
                return pubRec.ReasonCode;
            }

            if (_session is { } advancedSession)
            {
                await advancedSession.OutboundAdvancedAsync(id, cancellationToken).ConfigureAwait(false);
            }

            var reason = await ReleaseExchangeAsync(connection, id, cancellationToken).ConfigureAwait(false);
            if (_session is { } completedSession)
            {
                await completedSession.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
            }

            settled = true;
            return reason;
        }
        catch (MqttPacketTooLargeException)
        {
            // Rejected before any byte reached the wire: the entry was removed and the id is free.
            settled = true;
            throw;
        }
        finally
        {
            // Release the receive-maximum slot only when the id is also released — when the exchange
            // truly settled, or there is no session to resume it. A cancelled-but-on-the-wire publish on
            // a persistent session parks both: the broker still counts the message against its Receive
            // Maximum, so freeing the slot early would let a later burst exceed it. The connection drop
            // on the next reconnect re-arms the quota.
            if (quotaHeld && (settled || _session is null))
            {
                quota!.Release();
            }

            if (settled || _session is null)
            {
                _packetIds.Return(id);
            }
        }
    }

    // Sends an identified QoS 1/2 PUBLISH, recording it in the persistent session first so a
    // mid-exchange loss can resume. The caller holds the receive-maximum send quota across the
    // whole exchange.
    private async Task<MqttPacket> ExchangeAsync(
        MqttConnection connection,
        MqttPublishPacket packet,
        ushort id,
        MqttInFlightStage stage,
        CancellationToken cancellationToken)
    {
        if (_session is { } session)
        {
            await session.OutboundSentAsync(packet, stage, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            return await RequestAsync(connection, packet, id, cancellationToken).ConfigureAwait(false);
        }
        catch (MqttPacketTooLargeException)
        {
            // Never reached the wire: nothing to resume.
            if (_session is { } oversized)
            {
                await oversized.OutboundCompletedAsync(id, CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
        catch (OperationCanceledException)
        {
            // The caller cancelled, but the packet may be on the wire — the session keeps
            // it and resumes it like any other interrupted exchange.
            throw;
        }
        catch (Exception error) when (_session is not null)
        {
            throw new MqttPublishInFlightException(id, error);
        }
    }

    // The PUBREL → PUBCOMP tail of a QoS 2 exchange (still within the receive-maximum window: the
    // send quota frees only at PUBCOMP, the final acknowledgement, per MQTT 5 §4.9).
    private async Task<MqttReasonCode> ReleaseExchangeAsync(MqttConnection connection, ushort id, CancellationToken cancellationToken)
    {
        var release = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubRel,
            PacketIdentifier = id,
            ProtocolVersion = _protocolVersion,
        };

        MqttPacket second;
        try
        {
            second = await RequestAsync(connection, release, id, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error) when (_session is not null)
        {
            throw new MqttPublishInFlightException(id, error);
        }

        return second is MqttPublishAckPacket { PacketType: MqttPacketType.PubComp } pubComp
            ? pubComp.ReasonCode
            : throw new MqttProtocolException($"Expected a PUBCOMP for {id} but received {second.GetType().Name}.");
    }

    /// <summary>
    /// Resumes the unfinished exchanges of a presented session, oldest first: interrupted
    /// publishes re-send with DUP under their original identifiers, and exchanges that already
    /// have their PUBREC re-send only the PUBREL. Call after a session-aware connect that
    /// restored a session; without one this is a no-op.
    /// </summary>
    public async Task RedeliverAsync(CancellationToken cancellationToken)
    {
        if (_session is not { } session)
        {
            return;
        }

        var connection = ConnectedOrThrow();
        foreach (var entry in session.Snapshot().Outbound)
        {
            var id = entry.Packet.PacketIdentifier!.Value;
            try
            {
                switch (entry.Stage)
                {
                    case MqttInFlightStage.AwaitingPubAck:
                    {
                        var dup = entry.Packet with { Dup = true, ProtocolVersion = _protocolVersion };
                        var response = await ExchangeAsync(
                            connection, dup, id, MqttInFlightStage.AwaitingPubAck, cancellationToken).ConfigureAwait(false);
                        if (response is not MqttPublishAckPacket { PacketType: MqttPacketType.PubAck })
                        {
                            throw new MqttProtocolException($"Expected a PUBACK for {id} but received {response.GetType().Name}.");
                        }

                        await session.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
                        break;
                    }

                    case MqttInFlightStage.AwaitingPubRec:
                    {
                        var dup = entry.Packet with { Dup = true, ProtocolVersion = _protocolVersion };
                        var first = await ExchangeAsync(
                            connection, dup, id, MqttInFlightStage.AwaitingPubRec, cancellationToken).ConfigureAwait(false);
                        if (first is not MqttPublishAckPacket { PacketType: MqttPacketType.PubRec } pubRec)
                        {
                            throw new MqttProtocolException($"Expected a PUBREC for {id} but received {first.GetType().Name}.");
                        }

                        if ((byte)pubRec.ReasonCode >= 0x80)
                        {
                            await session.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
                            break;
                        }

                        await session.OutboundAdvancedAsync(id, cancellationToken).ConfigureAwait(false);
                        await ReleaseExchangeAsync(connection, id, cancellationToken).ConfigureAwait(false);
                        await session.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
                        break;
                    }

                    case MqttInFlightStage.AwaitingPubComp:
                        await ReleaseExchangeAsync(connection, id, cancellationToken).ConfigureAwait(false);
                        await session.OutboundCompletedAsync(id, cancellationToken).ConfigureAwait(false);
                        break;
                }

                _packetIds.Return(id);
            }
            catch (MqttPacketTooLargeException)
            {
                // This broker accepts smaller packets than the one being resumed. Retrying can
                // never succeed, so the exchange is already dropped from the session by
                // ExchangeAsync; free its id and keep redelivering the rest in order rather than
                // aborting the whole pass (which would re-loop the supervisor).
                _packetIds.Return(id);
            }

            // Any other failure here is a connection drop mid-redelivery: leave the id reserved
            // and the remaining entries in the session so the next resume continues, in order.
        }
    }

    /// <summary>
    /// Starts a client-initiated re-authentication on the live connection and completes when
    /// the broker concludes the exchange with an AUTH success.
    /// </summary>
    /// <exception cref="InvalidOperationException">No authenticator is configured, or an exchange is already running.</exception>
    /// <exception cref="MqttException">The broker did not conclude in time or the connection closed.</exception>
    public async Task ReAuthenticateAsync(CancellationToken cancellationToken)
    {
        var connection = ConnectedOrThrow();
        var authenticator = _options.Authenticator
            ?? throw new InvalidOperationException("Configure an authenticator to use re-authentication.");

        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (Interlocked.CompareExchange(ref _reauthSignal, signal, null) is not null)
        {
            throw new InvalidOperationException("A re-authentication exchange is already in progress.");
        }

        try
        {
            var data = await authenticator.NextDataAsync(null, cancellationToken).ConfigureAwait(false);
            await SendThroughAsync(
                connection,
                new MqttAuthPacket
                {
                    ReasonCode = MqttReasonCode.ReAuthenticate,
                    AuthenticationMethod = authenticator.Method,
                    AuthenticationData = data,
                },
                cancellationToken).ConfigureAwait(false);

            try
            {
                await signal.Task.WaitAsync(_options.AcknowledgementTimeout, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // The broker never concluded the exchange: faulting the connection both surfaces the
                // failure and prevents a late AUTH success from completing a subsequent re-authentication
                // (the next attempt runs on a fresh connection, not this stale signal slot).
                var error = new MqttException("The broker did not conclude the re-authentication in time.");
                await FaultConnectionAsync(connection, error).ConfigureAwait(false);
                throw error;
            }
        }
        finally
        {
            _reauthSignal = null;
        }
    }

    // Runs on the pump: answers broker challenges mid-session and completes a pending
    // re-authentication when the broker concludes with success.
    private async ValueTask HandleAuthAsync(MqttConnection connection, MqttAuthPacket auth, CancellationToken cancellationToken)
    {
        switch (auth.ReasonCode)
        {
            case MqttReasonCode.ContinueAuthentication when _options.Authenticator is { } authenticator:
                var data = await authenticator.NextDataAsync(auth.AuthenticationData, cancellationToken).ConfigureAwait(false);
                await SendThroughAsync(
                    connection,
                    new MqttAuthPacket
                    {
                        ReasonCode = MqttReasonCode.ContinueAuthentication,
                        AuthenticationMethod = authenticator.Method,
                        AuthenticationData = data,
                    },
                    cancellationToken).ConfigureAwait(false);
                break;

            case MqttReasonCode.ContinueAuthentication:
                throw new MqttProtocolException("The broker started an authentication exchange, but no authenticator is configured.");

            case MqttReasonCode.Success:
                _reauthSignal?.TrySetResult();
                break;
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
                // The packet is on the wire but the broker never acknowledged it within the window, so
                // it still owns the id and counts the message. Fault the connection to recover cleanly.
                var error = new MqttException($"The broker did not acknowledge packet {id} in time.");
                await FaultConnectionAsync(connection, error).ConfigureAwait(false);
                throw error;
            }
            catch (OperationCanceledException)
            {
                // A persistent session parks the id and keeps the message in flight for redelivery while
                // the connection (which is healthy — the caller cancelled, the broker did not fail) keeps
                // running. A clean-start client has no session to track the still-owned id, so the
                // connection is faulted to reset it; the broker drops the exchange on disconnect.
                if (_session is null)
                {
                    await FaultConnectionAsync(
                        connection,
                        new MqttException($"The wait for packet {id} was cancelled after it was sent.")).ConfigureAwait(false);
                }

                throw;
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
                            ReleaseAcceptedInboundQos2(pubRel.PacketIdentifier);
                            if (_session is { } pubRelSession)
                            {
                                await pubRelSession.InboundReleasedAsync(pubRel.PacketIdentifier, cancellationToken).ConfigureAwait(false);
                            }

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
                        case MqttAuthPacket auth:
                            await HandleAuthAsync(connection, auth, cancellationToken).ConfigureAwait(false);
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
        publish = ResolveInboundTopicAlias(publish);

        if (AcknowledgedMessageSink is { } acknowledgedSink)
        {
            await DeliverAcknowledgedAsync(connection, publish, acknowledgedSink, cancellationToken).ConfigureAwait(false);
            return;
        }

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
                var state = BeginInboundQos2(id, pendingAcknowledgement: false);
                if (state == InboundQos2ReceiveState.New)
                {
                    // Record before delivering: a crash after delivery but before the PUBREL
                    // must still suppress the broker's redelivery on resume.
                    if (_session is { } session)
                    {
                        await session.InboundReceivedAsync(id, cancellationToken).ConfigureAwait(false);
                    }

                    await DeliverAsync(publish, cancellationToken).ConfigureAwait(false);
                }

                if (state == InboundQos2ReceiveState.PendingAcknowledgement)
                {
                    break;
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

    private async ValueTask DeliverAcknowledgedAsync(
        MqttConnection connection,
        MqttPublishPacket publish,
        Func<MqttInboundPublishContext, CancellationToken, ValueTask> sink,
        CancellationToken cancellationToken)
    {
        switch (publish.QualityOfService)
        {
            case MqttQualityOfService.AtMostOnce:
                await sink(new MqttInboundPublishContext(publish, canReject: false, CompleteQos0Async), cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MqttQualityOfService.AtLeastOnce:
            {
                var id = RequiredId(publish);
                var canReject = _protocolVersion == MqttProtocolVersion.V500;
                await sink(
                        new MqttInboundPublishContext(
                            publish,
                            canReject,
                            (reason, reasonString, token) => SendPublishAckAsync(
                                connection,
                                MqttPacketType.PubAck,
                                id,
                                reason,
                                reasonString,
                                token)),
                        cancellationToken)
                    .ConfigureAwait(false);
                break;
            }

            case MqttQualityOfService.ExactlyOnce:
            {
                var id = RequiredId(publish);
                var state = BeginInboundQos2(id, pendingAcknowledgement: true);
                if (state == InboundQos2ReceiveState.Accepted)
                {
                    await SendPublishAckAsync(
                            connection,
                            MqttPacketType.PubRec,
                            id,
                            MqttReasonCode.Success,
                            reasonString: null,
                            cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                if (state == InboundQos2ReceiveState.PendingAcknowledgement)
                {
                    return;
                }

                await sink(
                        new MqttInboundPublishContext(
                            publish,
                            canReject: _protocolVersion == MqttProtocolVersion.V500,
                            async (reason, reasonString, token) =>
                            {
                                if ((byte)reason < 0x80 && _session is { } acceptedSession)
                                {
                                    await acceptedSession.InboundReceivedAsync(id, token).ConfigureAwait(false);
                                }

                                CompletePendingInboundQos2(id, accepted: (byte)reason < 0x80);

                                await SendPublishAckAsync(
                                        connection,
                                        MqttPacketType.PubRec,
                                        id,
                                        reason,
                                        reasonString,
                                        token)
                                    .ConfigureAwait(false);
                            }),
                        cancellationToken)
                    .ConfigureAwait(false);
                break;
            }
        }

        static ushort RequiredId(MqttPublishPacket publish) =>
            publish.PacketIdentifier
            ?? throw new MqttProtocolException("A QoS > 0 PUBLISH must carry a packet identifier.");

        static ValueTask CompleteQos0Async(
            MqttReasonCode reason,
            string? reasonString,
            CancellationToken token) =>
            ValueTask.CompletedTask;
    }

    private ValueTask SendPublishAckAsync(
        MqttConnection connection,
        MqttPacketType packetType,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        string? reasonString,
        CancellationToken cancellationToken) =>
        SendThroughAsync(
            connection,
            new MqttPublishAckPacket
            {
                PacketType = packetType,
                PacketIdentifier = packetIdentifier,
                ReasonCode = reasonCode,
                ReasonString = reasonString,
                ProtocolVersion = _protocolVersion,
            },
            cancellationToken);

    private enum InboundQos2ReceiveState
    {
        New,
        PendingAcknowledgement,
        Accepted,
    }

    private InboundQos2ReceiveState BeginInboundQos2(ushort packetIdentifier, bool pendingAcknowledgement)
    {
        lock (_inboundQos2Gate)
        {
            if (_inboundQos2.Contains(packetIdentifier))
            {
                return InboundQos2ReceiveState.Accepted;
            }

            if (_pendingAcknowledgedInboundQos2.Contains(packetIdentifier))
            {
                return InboundQos2ReceiveState.PendingAcknowledgement;
            }

            if (pendingAcknowledgement)
            {
                _pendingAcknowledgedInboundQos2.Add(packetIdentifier);
            }
            else
            {
                _inboundQos2.Add(packetIdentifier);
            }

            return InboundQos2ReceiveState.New;
        }
    }

    private void RestoreAcceptedInboundQos2(ushort packetIdentifier)
    {
        lock (_inboundQos2Gate)
        {
            _inboundQos2.Add(packetIdentifier);
        }
    }

    private void CompletePendingInboundQos2(ushort packetIdentifier, bool accepted)
    {
        lock (_inboundQos2Gate)
        {
            _pendingAcknowledgedInboundQos2.Remove(packetIdentifier);
            if (accepted)
            {
                _inboundQos2.Add(packetIdentifier);
            }
            else
            {
                _inboundQos2.Remove(packetIdentifier);
            }
        }
    }

    private void ReleaseAcceptedInboundQos2(ushort packetIdentifier)
    {
        lock (_inboundQos2Gate)
        {
            _inboundQos2.Remove(packetIdentifier);
        }
    }

    private ValueTask DeliverAsync(MqttPublishPacket publish, CancellationToken cancellationToken) =>
        MessageSink is { } sink
            ? sink(publish, cancellationToken)
            : _messages.Writer.WriteAsync(publish, cancellationToken);

    // Runs inside the connection's send lock, so assignment order and wire order always agree:
    // the packet that establishes an alias reaches the broker before any packet that uses it.
    private MqttPacket ApplyOutboundTopicAlias(MqttPacket packet)
    {
        if (packet is not MqttPublishPacket publish
            || publish.TopicAlias is not null
            || _outboundAliases is not { } aliases)
        {
            return packet;
        }

        if (aliases.TryGetValue(publish.Topic, out var alias))
        {
            return publish with { Topic = string.Empty, TopicAlias = alias };
        }

        if (aliases.Count < _outboundAliasMaximum)
        {
            alias = (ushort)(aliases.Count + 1);
            aliases[publish.Topic] = alias;
            return publish with { TopicAlias = alias };
        }

        return packet;
    }

    // Runs on the pump. The specification makes inbound resolution mandatory once the client
    // advertised a topic-alias maximum; violations are protocol errors that fault the session.
    private MqttPublishPacket ResolveInboundTopicAlias(MqttPublishPacket publish)
    {
        if (publish.TopicAlias is not { } alias)
        {
            if (publish.Topic.Length == 0)
            {
                throw new MqttProtocolException("A PUBLISH without a topic must carry a topic alias.");
            }

            return publish;
        }

        if (_inboundAliasTopics is not { } aliasTopics)
        {
            throw new MqttProtocolException("The broker sent a topic alias, but none were negotiated.");
        }

        if (alias == 0 || alias > aliasTopics.Length)
        {
            throw new MqttProtocolException($"Topic alias {alias} is outside the negotiated range of 1-{aliasTopics.Length}.");
        }

        if (publish.Topic.Length > 0)
        {
            aliasTopics[alias - 1] = publish.Topic;
            return publish;
        }

        return aliasTopics[alias - 1] is { } topic
            ? publish with { Topic = topic }
            : throw new MqttProtocolException($"Topic alias {alias} was used before it was established.");
    }

    private void CompletePending(ushort id, MqttPacket response)
    {
        if (_pending.TryGetValue(id, out var pending))
        {
            pending.TrySetResult(response);
        }
    }

    private void FailPending(Exception error)
    {
        _reauthSignal?.TrySetException(error);
        foreach (var entry in _pending)
        {
            entry.Value.TrySetException(error);
        }
    }

    // Tears the connection down the way the keep-alive loop does on a ping-response timeout. An
    // exchange that reached the wire but went unacknowledged — the broker too slow, or the caller
    // abandoning the wait — leaves the broker still owning the packet id and counting a QoS > 0 PUBLISH
    // against its Receive Maximum, so the connection is no longer in a known-good state. Faulting it
    // makes the supervisor reconnect, which re-arms the send quota and the packet-id allocator and
    // redelivers a persistent session's in-flight work.
    private async ValueTask FaultConnectionAsync(MqttConnection connection, Exception error)
    {
        _messages.Writer.TryComplete(error);
        FailPending(error);
        await connection.DisposeAsync().ConfigureAwait(false);
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
                    await FaultConnectionAsync(
                        connection,
                        new MqttException("The broker did not answer a PINGREQ within the keep-alive window.")).ConfigureAwait(false);
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
