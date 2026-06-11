using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttPublishAckCodecTests
{
    [Fact]
    public void V5_puback_with_reason_and_properties_round_trips()
    {
        var original = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubAck,
            PacketIdentifier = 1234,
            ReasonCode = MqttReasonCode.NotAuthorized,
            ProtocolVersion = MqttProtocolVersion.V500,
            ReasonString = "nope",
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.PacketType.ShouldBe(MqttPacketType.PubAck);
        decoded.PacketIdentifier.ShouldBe((ushort)1234);
        decoded.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        decoded.ReasonString.ShouldBe("nope");
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void V5_success_with_no_properties_is_a_two_byte_body()
    {
        var original = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubRec,
            PacketIdentifier = 9,
            ReasonCode = MqttReasonCode.Success,
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.RemainingLength.ShouldBe(2);
        decoded.ReasonCode.ShouldBe(MqttReasonCode.Success);
        decoded.ReasonString.ShouldBeNull();
    }

    [Fact]
    public void V5_reason_without_properties_is_a_three_byte_body()
    {
        var original = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubComp,
            PacketIdentifier = 5,
            ReasonCode = MqttReasonCode.PacketTooLarge,
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.RemainingLength.ShouldBe(3);
        decoded.ReasonCode.ShouldBe(MqttReasonCode.PacketTooLarge);
    }

    [Fact]
    public void V311_ack_is_always_a_two_byte_body()
    {
        var original = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubAck,
            PacketIdentifier = 77,
            ReasonCode = MqttReasonCode.Success,
            ProtocolVersion = MqttProtocolVersion.V311,
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.RemainingLength.ShouldBe(2);
        decoded.PacketIdentifier.ShouldBe((ushort)77);
    }

    [Fact]
    public void Pubrel_uses_fixed_header_flags_2()
    {
        var original = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubRel,
            PacketIdentifier = 3,
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.Flags.ShouldBe((byte)0x02);
        decoded.PacketType.ShouldBe(MqttPacketType.PubRel);
    }

    [Fact]
    public void Encode_rejects_non_ack_packet_type()
    {
        var packet = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.Publish,
            PacketIdentifier = 1,
        };

        Should.Throw<ArgumentException>(() => MqttPublishAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));
    }

    private static (MqttFixedHeader Header, MqttPublishAckPacket Packet) EncodeThenDecode(MqttPublishAckPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttPublishAckCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        consumed.ShouldBe(output.WrittenCount);

        return (header, MqttPublishAckCodec.Decode(header, body, packet.ProtocolVersion));
    }
}
