using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Connection;

/// <summary>
/// The raw packet engine over a connected <see cref="IMqttTransport"/>: a serialized send path and
/// a receive loop that frames, decodes, and queues inbound packets. Higher layers add the
/// handshake, keepalive, and QoS state on top. Call <see cref="Start"/> once after construction.
/// </summary>
public sealed class MqttConnection : IAsyncDisposable
{
    private readonly IMqttTransport _transport;
    private readonly MqttConnectionOptions _options;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly Channel<MqttPacket> _inbound;
    private readonly CancellationTokenSource _lifetime = new();
    private Task? _receiveLoop;
    private volatile bool _disposed;

    /// <summary>Creates an engine over <paramref name="transport"/>. The connection owns the transport.</summary>
    public MqttConnection(IMqttTransport transport, MqttConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(options);

        _transport = transport;
        _options = options;
        _inbound = Channel.CreateBounded<MqttPacket>(new BoundedChannelOptions(options.InboundQueueCapacity)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    /// <summary>
    /// Inbound packets in arrival order. Completes when the peer closes the connection; completes
    /// with an <see cref="MqttProtocolException"/> when the peer violates the protocol.
    /// </summary>
    public ChannelReader<MqttPacket> Inbound => _inbound.Reader;

    /// <summary>
    /// An optional handler invoked on the receive loop for every decoded packet. Return
    /// <see langword="true"/> to consume the packet without queueing it to <see cref="Inbound"/>
    /// — one less task hop for latency-sensitive packets like acknowledgements. The handler runs
    /// on the receive loop, so it must be fast and must not block or send.
    /// </summary>
    public Func<MqttPacket, bool>? InboundFilter { get; set; }

    /// <summary>
    /// The broker's maximum packet size from its CONNACK, when it advertised one. While set,
    /// every outbound packet is size-checked before any byte reaches the wire and an oversized
    /// one fails with <see cref="MqttPacketTooLargeException"/>. Unset (the default) costs the
    /// send path nothing.
    /// </summary>
    public uint? MaximumOutboundPacketSize { get; set; }

    /// <summary>
    /// An optional rewrite applied to each outbound packet inside the send lock — wire order
    /// matches transform order exactly, which stateful rewrites like topic-alias assignment
    /// depend on. Keep it fast; it runs under the lock that serializes all sends.
    /// </summary>
    public Func<MqttPacket, MqttPacket>? OutboundTransform { get; set; }

    /// <summary>Completes when the receive loop has stopped, for orderly shutdown.</summary>
    public Task Completion => _receiveLoop ?? Task.CompletedTask;

    /// <summary>Starts the receive loop. Call exactly once.</summary>
    /// <exception cref="InvalidOperationException">The connection was already started.</exception>
    public void Start()
    {
        if (_receiveLoop is not null)
        {
            throw new InvalidOperationException("The connection is already started.");
        }

        _receiveLoop = Task.Run(() => ReceiveLoopAsync(_lifetime.Token), CancellationToken.None);
    }

    /// <summary>Encodes and sends one packet. Concurrent senders are serialized in call order.</summary>
    public async ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _sendLock.WaitScopedAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (OutboundTransform is { } transform)
            {
                packet = transform(packet);
            }

            if (MaximumOutboundPacketSize is { } limit)
            {
                // The broker bounded what it accepts: stage the packet to learn its encoded
                // size and reject an oversized one before any byte reaches the wire.
                using var staging = new PooledBufferWriter();
                MqttPacketWriter.Write(staging, packet);
                if ((uint)staging.WrittenCount > limit)
                {
                    throw new MqttPacketTooLargeException(staging.WrittenCount, limit);
                }

                var destination = _transport.Output.GetSpan(staging.WrittenCount);
                staging.WrittenSpan.CopyTo(destination);
                _transport.Output.Advance(staging.WrittenCount);
            }
            else
            {
                MqttPacketWriter.Write(_transport.Output, packet);
            }

            var result = await _transport.Output.FlushAsync(cancellationToken).ConfigureAwait(false);
            if (result.IsCompleted)
            {
                throw new MqttException("The transport closed while sending.");
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>Stops the loops, closes the transport, and completes the inbound queue.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _lifetime.CancelAsync().ConfigureAwait(false);

        if (_receiveLoop is { } loop)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Shutdown path.
            }
        }

        await _transport.DisposeAsync().ConfigureAwait(false);
        _inbound.Writer.TryComplete();
        _sendLock.Dispose();
        _lifetime.Dispose();
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                var result = await _transport.Input.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;

                while (TryDecodePacket(ref buffer, out var packet))
                {
                    if (InboundFilter?.Invoke(packet) == true)
                    {
                        continue;
                    }

                    await _inbound.Writer.WriteAsync(packet, cancellationToken).ConfigureAwait(false);
                }

                _transport.Input.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                {
                    if (buffer.Length > 0)
                    {
                        throw new MqttProtocolException("The peer closed the connection mid-packet.");
                    }

                    break;
                }
            }

            _inbound.Writer.TryComplete();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _inbound.Writer.TryComplete();
        }
        catch (Exception error)
        {
            _inbound.Writer.TryComplete(error);
        }
    }

    private bool TryDecodePacket(ref ReadOnlySequence<byte> buffer, out MqttPacket packet)
    {
        packet = null!;

        if (buffer.IsEmpty)
        {
            return false;
        }

        // The fixed header is at most five bytes (type byte + four-byte remaining length).
        Span<byte> headerBytes = stackalloc byte[5];
        var peekLength = (int)Math.Min(buffer.Length, headerBytes.Length);
        buffer.Slice(0, peekLength).CopyTo(headerBytes);

        var status = MqttFrameReader.TryReadHeader(headerBytes[..peekLength], out var header, out var headerLength);
        if (status == MqttFrameStatus.Malformed)
        {
            throw new MqttProtocolException("Malformed fixed header.");
        }

        if (status == MqttFrameStatus.Incomplete)
        {
            return false;
        }

        var totalLength = (long)headerLength + header.RemainingLength;
        if (totalLength > _options.MaxInboundPacketSize)
        {
            throw new MqttProtocolException(
                $"Inbound packet of {totalLength} bytes exceeds the {_options.MaxInboundPacketSize}-byte limit.");
        }

        if (buffer.Length < totalLength)
        {
            return false;
        }

        var body = buffer.Slice(headerLength, header.RemainingLength);
        packet = DecodeBody(header, body);
        buffer = buffer.Slice(totalLength);
        return true;
    }

    private MqttPacket DecodeBody(MqttFixedHeader header, ReadOnlySequence<byte> body)
    {
        if (body.IsSingleSegment)
        {
            return MqttPacketDecoder.Decode(header, body.FirstSpan, _options.ProtocolVersion);
        }

        var length = (int)body.Length;
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            body.CopyTo(rented);
            return MqttPacketDecoder.Decode(header, rented.AsSpan(0, length), _options.ProtocolVersion);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
