using System.Buffers;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttFrameWriterTests
{
    [Fact]
    public void WriteHeader_encodes_remaining_length_as_varint()
    {
        var output = new ArrayBufferWriter<byte>();

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Connect, 0x00, 128));

        // Type 1, flags 0 -> 0x10; remaining length 128 -> 0x80 0x01.
        output.WrittenSpan.ToArray().ShouldBe(new byte[] { 0x10, 0x80, 0x01 });
    }

    [Fact]
    public void WriteHeader_round_trips_through_the_reader()
    {
        var output = new ArrayBufferWriter<byte>();
        var header = new MqttFixedHeader(MqttPacketType.Publish, 0x02, 130);

        MqttFrameWriter.WriteHeader(output, header);
        output.Write(new byte[130]); // a body so the frame is complete

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var parsed, out var body, out var consumed);

        status.ShouldBe(MqttFrameStatus.Complete);
        parsed.ShouldBe(header);
        body.Length.ShouldBe(130);
        consumed.ShouldBe(output.WrittenCount);
    }

    [Fact]
    public void WriteHeader_throws_when_remaining_length_negative()
    {
        var output = new ArrayBufferWriter<byte>();

        Should.Throw<ArgumentOutOfRangeException>(
            () => MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Connect, 0x00, -1)));
    }
}
