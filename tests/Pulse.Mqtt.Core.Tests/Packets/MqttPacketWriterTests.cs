using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttPacketWriterTests
{
    public static TheoryData<MqttPacket> AllPacketKinds() => new()
    {
        new MqttConnectPacket { ClientId = "c" },
        new MqttConnAckPacket(),
        new MqttPublishPacket { Topic = "t" },
        new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = 1 },
        new MqttSubscribePacket { PacketIdentifier = 1, TopicFilters = [new MqttTopicFilter("a")] },
        new MqttSubAckPacket { PacketIdentifier = 1, ReasonCodes = [MqttReasonCode.Success] },
        new MqttUnsubscribePacket { PacketIdentifier = 1, TopicFilters = ["a"] },
        new MqttUnsubAckPacket { PacketIdentifier = 1, ReasonCodes = [MqttReasonCode.Success] },
        new MqttPingReqPacket(),
        new MqttPingRespPacket(),
        new MqttDisconnectPacket(),
        new MqttAuthPacket { ReasonCode = MqttReasonCode.ContinueAuthentication, AuthenticationMethod = "m" },
    };

    [Theory]
    [MemberData(nameof(AllPacketKinds))]
    public void Every_packet_kind_round_trips_through_writer_and_decoder(MqttPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();

        MqttPacketWriter.Write(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        consumed.ShouldBe(output.WrittenCount);

        var decoded = MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
        decoded.GetType().ShouldBe(packet.GetType());
    }
}
