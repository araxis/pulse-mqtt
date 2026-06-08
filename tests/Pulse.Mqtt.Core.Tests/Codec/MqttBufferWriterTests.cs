using System.Buffers;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttBufferWriterTests
{
    [Fact]
    public void Writes_primitives_in_big_endian()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new MqttBufferWriter(output);

        writer.WriteByte(0x2A);
        writer.WriteUInt16(256);
        writer.WriteUInt32(5);

        output.WrittenSpan.ToArray().ShouldBe(new byte[] { 0x2A, 0x01, 0x00, 0x00, 0x00, 0x00, 0x05 });
    }

    [Fact]
    public void Writes_then_reads_back_a_composite_payload()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new MqttBufferWriter(output);

        writer.WriteByte(0x10);
        writer.WriteUInt16(40_000);
        writer.WriteVarInt(2_097_152);
        writer.WriteString("topic/a");
        writer.WriteBinary(new byte[] { 0x01, 0x02, 0x03 });

        var reader = new MqttBufferReader(output.WrittenSpan);
        reader.ReadByte().ShouldBe((byte)0x10);
        reader.ReadUInt16().ShouldBe((ushort)40_000);
        reader.ReadVarInt().ShouldBe(2_097_152u);
        reader.ReadString().ShouldBe("topic/a");
        reader.ReadBinary().ToArray().ShouldBe(new byte[] { 0x01, 0x02, 0x03 });
        reader.Remaining.ShouldBe(0);
    }

    [Fact]
    public void WriteString_throws_when_too_long()
    {
        Should.Throw<ArgumentException>(() =>
        {
            var output = new ArrayBufferWriter<byte>();
            var writer = new MqttBufferWriter(output);
            writer.WriteString(new string('x', ushort.MaxValue + 1));
        });
    }
}
