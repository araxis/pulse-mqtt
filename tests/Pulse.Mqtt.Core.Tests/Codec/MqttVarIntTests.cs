using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttVarIntTests
{
    [Theory]
    [InlineData(0u, new byte[] { 0x00 })]
    [InlineData(127u, new byte[] { 0x7F })]
    [InlineData(128u, new byte[] { 0x80, 0x01 })]
    [InlineData(16_383u, new byte[] { 0xFF, 0x7F })]
    [InlineData(16_384u, new byte[] { 0x80, 0x80, 0x01 })]
    [InlineData(2_097_151u, new byte[] { 0xFF, 0xFF, 0x7F })]
    [InlineData(2_097_152u, new byte[] { 0x80, 0x80, 0x80, 0x01 })]
    [InlineData(268_435_455u, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
    public void Write_matches_spec_vectors(uint value, byte[] expected)
    {
        Span<byte> buffer = stackalloc byte[MqttVarInt.MaxEncodedLength];

        var written = MqttVarInt.Write(buffer, value);

        written.ShouldBe(expected.Length);
        buffer[..written].ToArray().ShouldBe(expected);
    }

    [Theory]
    [InlineData(new byte[] { 0x00 }, 0u)]
    [InlineData(new byte[] { 0x80, 0x01 }, 128u)]
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0x7F }, 268_435_455u)]
    public void TryRead_matches_spec_vectors(byte[] encoded, uint expected)
    {
        var status = MqttVarInt.TryRead(encoded, out var value, out var bytesRead);

        status.ShouldBe(MqttVarIntStatus.Success);
        value.ShouldBe(expected);
        bytesRead.ShouldBe(encoded.Length);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(127u)]
    [InlineData(128u)]
    [InlineData(300u)]
    [InlineData(16_384u)]
    [InlineData(2_097_152u)]
    [InlineData(268_435_455u)]
    public void Write_then_TryRead_round_trips(uint value)
    {
        Span<byte> buffer = stackalloc byte[MqttVarInt.MaxEncodedLength];
        var written = MqttVarInt.Write(buffer, value);

        var status = MqttVarInt.TryRead(buffer[..written], out var read, out var bytesRead);

        status.ShouldBe(MqttVarIntStatus.Success);
        read.ShouldBe(value);
        bytesRead.ShouldBe(written);
    }

    [Fact]
    public void TryRead_reports_incomplete_when_continuation_runs_past_end()
    {
        var status = MqttVarInt.TryRead(new byte[] { 0x80 }, out _, out var bytesRead);

        status.ShouldBe(MqttVarIntStatus.Incomplete);
        bytesRead.ShouldBe(0);
    }

    [Fact]
    public void TryRead_reports_malformed_when_five_bytes_would_be_needed()
    {
        var status = MqttVarInt.TryRead(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, out _, out _);

        status.ShouldBe(MqttVarIntStatus.Malformed);
    }

    [Fact]
    public void GetEncodedLength_matches_boundaries()
    {
        MqttVarInt.GetEncodedLength(0).ShouldBe(1);
        MqttVarInt.GetEncodedLength(127).ShouldBe(1);
        MqttVarInt.GetEncodedLength(128).ShouldBe(2);
        MqttVarInt.GetEncodedLength(16_383).ShouldBe(2);
        MqttVarInt.GetEncodedLength(16_384).ShouldBe(3);
        MqttVarInt.GetEncodedLength(2_097_151).ShouldBe(3);
        MqttVarInt.GetEncodedLength(2_097_152).ShouldBe(4);
        MqttVarInt.GetEncodedLength((uint)MqttVarInt.MaxValue).ShouldBe(4);
    }

    [Fact]
    public void Write_throws_when_value_exceeds_max()
    {
        var buffer = new byte[MqttVarInt.MaxEncodedLength];

        Should.Throw<ArgumentOutOfRangeException>(() => MqttVarInt.Write(buffer, (uint)MqttVarInt.MaxValue + 1));
    }
}
