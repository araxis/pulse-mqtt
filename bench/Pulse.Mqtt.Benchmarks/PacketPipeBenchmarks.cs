using System.Buffers;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Single-packet pipe work: writing a small PUBLISH to a <see cref="PipeWriter"/> in one
/// encode-and-flush, and decoding a 10 KB PUBLISH arriving through a <see cref="PipeReader"/>.
/// </summary>
[MemoryDiagnoser]
public class PacketPipeBenchmarks
{
    private readonly MqttPublishPacket _smallPacket = new() { Topic = "topic", Payload = new byte[10] };
    private readonly MemoryStream _sendStream = new(1024);
    private byte[] _largeEncoded = [];

    [GlobalSetup]
    public void Setup()
    {
        var large = new MqttPublishPacket { Topic = "topic", Payload = new byte[10 * 1024] };
        var scratch = new ArrayBufferWriter<byte>(16 * 1024);
        MqttPacketWriter.Write(scratch, large);
        _largeEncoded = scratch.WrittenSpan.ToArray();
    }

    [Benchmark]
    public async ValueTask Send_Small_Packet()
    {
        _sendStream.Position = 0;
        var output = PipeWriter.Create(_sendStream, new StreamPipeWriterOptions(leaveOpen: true));

        MqttPacketWriter.Write(output, _smallPacket);
        await output.FlushAsync();
        await output.CompleteAsync();
    }

    [Benchmark]
    public async Task<MqttPacket> Decode_Large_Publish()
    {
        var input = PipeReader.Create(new MemoryStream(_largeEncoded, writable: false));

        while (true)
        {
            var result = await input.ReadAsync();
            var buffer = result.Buffer;

            if (TryDecode(buffer, out var packet, out var consumed))
            {
                input.AdvanceTo(buffer.GetPosition(consumed));
                await input.CompleteAsync();
                return packet;
            }

            if (result.IsCompleted)
            {
                throw new InvalidOperationException("The packet never completed.");
            }

            input.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private static bool TryDecode(in ReadOnlySequence<byte> buffer, out MqttPacket packet, out long consumed)
    {
        packet = null!;
        consumed = 0;

        Span<byte> headerBytes = stackalloc byte[5];
        var peekLength = (int)Math.Min(buffer.Length, headerBytes.Length);
        buffer.Slice(0, peekLength).CopyTo(headerBytes);

        if (MqttFrameReader.TryReadHeader(headerBytes[..peekLength], out var header, out var headerLength)
                != MqttFrameStatus.Complete
            || buffer.Length < headerLength + (long)header.RemainingLength)
        {
            return false;
        }

        var body = buffer.Slice(headerLength, header.RemainingLength);
        if (body.IsSingleSegment)
        {
            packet = MqttPacketDecoder.Decode(header, body.FirstSpan, MqttProtocolVersion.V500);
        }
        else
        {
            var rented = ArrayPool<byte>.Shared.Rent((int)body.Length);
            try
            {
                body.CopyTo(rented);
                packet = MqttPacketDecoder.Decode(header, rented.AsSpan(0, (int)body.Length), MqttProtocolVersion.V500);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        consumed = headerLength + (long)header.RemainingLength;
        return true;
    }
}
