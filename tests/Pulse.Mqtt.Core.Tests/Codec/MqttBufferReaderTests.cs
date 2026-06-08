using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttBufferReaderTests
{
    [Fact]
    public void Reads_primitives_in_order()
    {
        byte[] data =
        [
            0x2A,                   // byte 42
            0x01, 0x00,             // uint16 256
            0x00, 0x00, 0x00, 0x05, // uint32 5
        ];

        var reader = new MqttBufferReader(data);

        reader.ReadByte().ShouldBe((byte)0x2A);
        reader.ReadUInt16().ShouldBe((ushort)256);
        reader.ReadUInt32().ShouldBe(5u);
        reader.Remaining.ShouldBe(0);
        reader.Consumed.ShouldBe(7);
    }

    [Fact]
    public void Reads_length_prefixed_string()
    {
        byte[] data = [0x00, 0x05, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o'];

        var reader = new MqttBufferReader(data);

        reader.ReadString().ShouldBe("hello");
    }

    [Fact]
    public void Reads_empty_string()
    {
        var reader = new MqttBufferReader(new byte[] { 0x00, 0x00 });

        reader.ReadString().ShouldBe(string.Empty);
    }

    [Fact]
    public void ReadBinary_returns_slice_of_underlying_buffer()
    {
        byte[] data = [0x00, 0x03, 0xAA, 0xBB, 0xCC];

        var reader = new MqttBufferReader(data);
        var binary = reader.ReadBinary();

        binary.ToArray().ShouldBe(new byte[] { 0xAA, 0xBB, 0xCC });
    }

    [Fact]
    public void Throws_on_under_run()
    {
        Should.Throw<MqttProtocolException>(() =>
        {
            var reader = new MqttBufferReader(new byte[] { 0x00 });
            reader.ReadUInt16();
        });
    }

    [Fact]
    public void Throws_when_string_length_exceeds_buffer()
    {
        Should.Throw<MqttProtocolException>(() =>
        {
            var reader = new MqttBufferReader(new byte[] { 0x00, 0x05, (byte)'h', (byte)'i' });
            reader.ReadString();
        });
    }

    [Fact]
    public void Throws_on_invalid_utf8()
    {
        Should.Throw<MqttProtocolException>(() =>
        {
            var reader = new MqttBufferReader(new byte[] { 0x00, 0x01, 0xFF });
            reader.ReadString();
        });
    }
}
