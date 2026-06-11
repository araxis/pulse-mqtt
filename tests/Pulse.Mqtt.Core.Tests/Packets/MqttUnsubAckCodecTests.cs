using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttUnsubAckCodecTests
{
    [Fact]
    public void V5_unsuback_round_trips()
    {
        var original = new MqttUnsubAckPacket
        {
            PacketIdentifier = 21,
            ProtocolVersion = MqttProtocolVersion.V500,
            ReasonCodes = [MqttReasonCode.Success, MqttReasonCode.NotAuthorized],
            ReasonString = "mixed",
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.PacketType.ShouldBe(MqttPacketType.UnsubAck);
        decoded.PacketIdentifier.ShouldBe((ushort)21);
        decoded.ReasonCodes.ShouldBe(original.ReasonCodes);
        decoded.ReasonString.ShouldBe("mixed");
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void V311_unsuback_has_no_payload()
    {
        var original = new MqttUnsubAckPacket
        {
            PacketIdentifier = 9,
            ProtocolVersion = MqttProtocolVersion.V311,
            ReasonCodes = [],
        };

        var (header, decoded) = EncodeThenDecode(original);

        header.RemainingLength.ShouldBe(2);
        decoded.PacketIdentifier.ShouldBe((ushort)9);
        decoded.ReasonCodes.ShouldBeEmpty();
    }

    [Fact]
    public void Encode_rejects_v5_without_reason_codes()
    {
        var packet = new MqttUnsubAckPacket
        {
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V500,
            ReasonCodes = [],
        };

        Should.Throw<ArgumentException>(() => MqttUnsubAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));
    }

    private static (MqttFixedHeader Header, MqttUnsubAckPacket Packet) EncodeThenDecode(MqttUnsubAckPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttUnsubAckCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        consumed.ShouldBe(output.WrittenCount);

        return (header, MqttUnsubAckCodec.Decode(body, packet.ProtocolVersion));
    }
}
