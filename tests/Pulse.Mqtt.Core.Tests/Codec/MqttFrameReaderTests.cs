using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttFrameReaderTests
{
    [Fact]
    public void Reads_a_complete_frame_with_body()
    {
        // PUBLISH (type 3), flags 0, remaining length 3, body AA BB CC.
        byte[] buffer = [0x30, 0x03, 0xAA, 0xBB, 0xCC];

        var status = MqttFrameReader.TryReadFrame(buffer, out var header, out var body, out var consumed);

        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Publish);
        header.RemainingLength.ShouldBe(3);
        body.ToArray().ShouldBe(new byte[] { 0xAA, 0xBB, 0xCC });
        consumed.ShouldBe(5);
    }

    [Fact]
    public void Reads_a_zero_length_frame()
    {
        // PINGRESP (type 13), flags 0, remaining length 0.
        byte[] buffer = [0xD0, 0x00];

        var status = MqttFrameReader.TryReadFrame(buffer, out var header, out var body, out var consumed);

        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.PingResp);
        body.Length.ShouldBe(0);
        consumed.ShouldBe(2);
    }

    [Fact]
    public void Reports_incomplete_on_empty_buffer()
    {
        MqttFrameReader.TryReadFrame(ReadOnlySpan<byte>.Empty, out _, out _, out _)
            .ShouldBe(MqttFrameStatus.Incomplete);
    }

    [Fact]
    public void Reports_incomplete_when_remaining_length_is_truncated()
    {
        // Continuation bit set, but no following byte.
        byte[] buffer = [0x30, 0x80];

        MqttFrameReader.TryReadFrame(buffer, out _, out _, out _).ShouldBe(MqttFrameStatus.Incomplete);
    }

    [Fact]
    public void Reports_incomplete_when_body_not_fully_buffered()
    {
        // Declares 5 body bytes, only 2 present.
        byte[] buffer = [0x30, 0x05, 0xAA, 0xBB];

        var status = MqttFrameReader.TryReadFrame(buffer, out _, out _, out var consumed);

        status.ShouldBe(MqttFrameStatus.Incomplete);
        consumed.ShouldBe(0);
    }

    [Fact]
    public void Reports_malformed_on_zero_packet_type()
    {
        byte[] buffer = [0x00, 0x00];

        MqttFrameReader.TryReadFrame(buffer, out _, out _, out _).ShouldBe(MqttFrameStatus.Malformed);
    }

    [Fact]
    public void Reports_malformed_when_pubrel_flags_are_wrong()
    {
        // PUBREL (type 6) requires flags 0b0010; 0b0000 is invalid.
        byte[] buffer = [0x60, 0x00];

        MqttFrameReader.TryReadFrame(buffer, out _, out _, out _).ShouldBe(MqttFrameStatus.Malformed);
    }

    [Fact]
    public void Reports_malformed_when_subscribe_flags_are_wrong()
    {
        // SUBSCRIBE (type 8) requires flags 0b0010; 0b0000 is invalid.
        byte[] buffer = [0x80, 0x00];

        MqttFrameReader.TryReadFrame(buffer, out _, out _, out _).ShouldBe(MqttFrameStatus.Malformed);
    }

    [Fact]
    public void Reads_only_the_first_frame_from_a_multi_packet_buffer()
    {
        // Frame A: PUBLISH len 1 body 0x01. Frame B: PINGRESP.
        byte[] buffer = [0x30, 0x01, 0x01, 0xD0, 0x00];

        var first = MqttFrameReader.TryReadFrame(buffer, out _, out var bodyA, out var consumedA);

        first.ShouldBe(MqttFrameStatus.Complete);
        consumedA.ShouldBe(3);
        bodyA.ToArray().ShouldBe(new byte[] { 0x01 });

        var second = MqttFrameReader.TryReadFrame(buffer.AsSpan(consumedA), out var headerB, out _, out var consumedB);

        second.ShouldBe(MqttFrameStatus.Complete);
        headerB.PacketType.ShouldBe(MqttPacketType.PingResp);
        consumedB.ShouldBe(2);
    }
}
