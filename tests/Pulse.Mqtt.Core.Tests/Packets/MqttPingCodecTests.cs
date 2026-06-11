using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttPingCodecTests
{
    [Fact]
    public void WriteRequest_emits_two_bytes()
    {
        var output = new ArrayBufferWriter<byte>();

        MqttPingCodec.WriteRequest(output);

        output.WrittenSpan.ToArray().ShouldBe(new byte[] { 0xC0, 0x00 });
    }

    [Fact]
    public void WriteResponse_emits_two_bytes()
    {
        var output = new ArrayBufferWriter<byte>();

        MqttPingCodec.WriteResponse(output);

        output.WrittenSpan.ToArray().ShouldBe(new byte[] { 0xD0, 0x00 });
    }

    [Fact]
    public void Request_frame_reads_with_empty_body()
    {
        var output = new ArrayBufferWriter<byte>();
        MqttPingCodec.WriteRequest(output);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out _);

        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.PingReq);
        body.IsEmpty.ShouldBeTrue();
        MqttPingCodec.EnsureEmptyBody(body); // throws on a non-empty body, failing the test
    }

    [Fact]
    public void EnsureEmptyBody_throws_on_non_empty()
    {
        Should.Throw<MqttProtocolException>(() => MqttPingCodec.EnsureEmptyBody(new byte[] { 0x01 }));
    }
}
