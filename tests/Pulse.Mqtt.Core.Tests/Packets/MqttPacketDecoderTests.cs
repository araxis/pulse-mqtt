using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttPacketDecoderTests
{
    [Fact]
    public void Dispatches_connect() =>
        Decode(o => MqttConnectCodec.Encode(o, new MqttConnectPacket { ClientId = "c" })).ShouldBeOfType<MqttConnectPacket>();

    [Fact]
    public void Dispatches_connack() =>
        Decode(o => MqttConnAckCodec.Encode(o, new MqttConnAckPacket())).ShouldBeOfType<MqttConnAckPacket>();

    [Fact]
    public void Dispatches_publish() =>
        Decode(o => MqttPublishCodec.Encode(o, new MqttPublishPacket { Topic = "t" })).ShouldBeOfType<MqttPublishPacket>();

    [Theory]
    [InlineData(MqttPacketType.PubAck)]
    [InlineData(MqttPacketType.PubRec)]
    [InlineData(MqttPacketType.PubRel)]
    [InlineData(MqttPacketType.PubComp)]
    public void Dispatches_publish_acks(MqttPacketType packetType) =>
        Decode(o => MqttPublishAckCodec.Encode(o, new MqttPublishAckPacket { PacketType = packetType, PacketIdentifier = 1 }))
            .ShouldBeOfType<MqttPublishAckPacket>()
            .PacketType.ShouldBe(packetType);

    [Fact]
    public void Dispatches_subscribe() =>
        Decode(o => MqttSubscribeCodec.Encode(o, new MqttSubscribePacket { PacketIdentifier = 1, TopicFilters = [new MqttTopicFilter("a")] }))
            .ShouldBeOfType<MqttSubscribePacket>();

    [Fact]
    public void Dispatches_suback() =>
        Decode(o => MqttSubAckCodec.Encode(o, new MqttSubAckPacket { PacketIdentifier = 1, ReasonCodes = [MqttReasonCode.Success] }))
            .ShouldBeOfType<MqttSubAckPacket>();

    [Fact]
    public void Dispatches_unsubscribe() =>
        Decode(o => MqttUnsubscribeCodec.Encode(o, new MqttUnsubscribePacket { PacketIdentifier = 1, TopicFilters = ["a"] }))
            .ShouldBeOfType<MqttUnsubscribePacket>();

    [Fact]
    public void Dispatches_unsuback() =>
        Decode(o => MqttUnsubAckCodec.Encode(o, new MqttUnsubAckPacket { PacketIdentifier = 1, ReasonCodes = [MqttReasonCode.Success] }))
            .ShouldBeOfType<MqttUnsubAckPacket>();

    [Fact]
    public void Dispatches_pingreq() =>
        Decode(o => MqttPingCodec.WriteRequest(o)).ShouldBeOfType<MqttPingReqPacket>();

    [Fact]
    public void Dispatches_pingresp() =>
        Decode(o => MqttPingCodec.WriteResponse(o)).ShouldBeOfType<MqttPingRespPacket>();

    [Fact]
    public void Dispatches_disconnect() =>
        Decode(o => MqttDisconnectCodec.Encode(o, new MqttDisconnectPacket())).ShouldBeOfType<MqttDisconnectPacket>();

    [Fact]
    public void Dispatches_auth() =>
        Decode(o => MqttAuthCodec.Encode(o, new MqttAuthPacket { ReasonCode = MqttReasonCode.ContinueAuthentication, AuthenticationMethod = "m" }))
            .ShouldBeOfType<MqttAuthPacket>();

    private static MqttPacket Decode(Action<ArrayBufferWriter<byte>> encode)
    {
        var output = new ArrayBufferWriter<byte>();
        encode(output);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out _);
        status.ShouldBe(MqttFrameStatus.Complete);

        return MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
    }
}
