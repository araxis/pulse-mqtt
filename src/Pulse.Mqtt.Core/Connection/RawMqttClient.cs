using System.Threading.Channels;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Connection;

/// <summary>
/// A non-resilient MQTT client: one connection, the CONNECT/CONNACK handshake, and protocol
/// keep-alive. Received application messages surface on <see cref="Messages"/>. Reconnection,
/// re-subscription, and offline queueing live in the resilient layer built on top of this type.
/// </summary>
public sealed class RawMqttClient : IAsyncDisposable
{
    private readonly IMqttTransportFactory _transportFactory;
    private readonly RawMqttClientOptions _options;
    private readonly TimeProvider _time;
    private readonly Channel<MqttPublishPacket> _messages;
    private readonly CancellationTokenSource _lifetime = new();

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
        _lifetime.Dispose();
    }

    private async ValueTask SendThroughAsync(MqttConnection connection, MqttPacket packet, CancellationToken cancellationToken)
    {
        await connection.SendAsync(packet, cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _lastSendTimestamp, _time.GetTimestamp());
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
                            await _messages.Writer.WriteAsync(publish, cancellationToken).ConfigureAwait(false);
                            break;
                        case MqttPingRespPacket:
                            _pongSignal?.TrySetResult();
                            break;
                        default:
                            // Acknowledgement correlation arrives with the QoS layer.
                            break;
                    }
                }
            }

            _messages.Writer.TryComplete();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _messages.Writer.TryComplete();
        }
        catch (ChannelClosedException ex)
        {
            _messages.Writer.TryComplete(ex.InnerException ?? ex);
        }
        catch (Exception ex)
        {
            _messages.Writer.TryComplete(ex);
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
                    _messages.Writer.TryComplete(
                        new MqttException("The broker did not answer a PINGREQ within the keep-alive window."));
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
