using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttSubAckCodecTests
{
    [Fact]
    public void V5_suback_round_trips()
    {
        var original = new MqttSubAckPacket
        {
            PacketIdentifier = 11,
            ProtocolVersion = MqttProtocolVersion.V500,
            ReasonCodes = [MqttReasonCode.Success, (MqttReasonCode)0x02, MqttReasonCode.NotAuthorized],
            ReasonString = "partial",
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var decoded = EncodeThenDecode(original);

        decoded.PacketIdentifier.ShouldBe((ushort)11);
        decoded.ReasonCodes.ShouldBe(original.ReasonCodes);
        decoded.ReasonString.ShouldBe("partial");
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void V311_suback_round_trips()
    {
        var original = new MqttSubAckPacket
        {
            PacketIdentifier = 7,
            ProtocolVersion = MqttProtocolVersion.V311,
            ReasonCodes = [MqttReasonCode.Success],
        };

        var decoded = EncodeThenDecode(original);

        decoded.PacketIdentifier.ShouldBe((ushort)7);
        decoded.ReasonCodes.ShouldHaveSingleItem().ShouldBe(MqttReasonCode.Success);
        decoded.ReasonString.ShouldBeNull();
    }

    [Fact]
    public void Encode_rejects_empty_reason_codes()
    {
        var packet = new MqttSubAckPacket { PacketIdentifier = 1, ReasonCodes = [] };

        Should.Throw<ArgumentException>(() => MqttSubAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));
    }

    private static MqttSubAckPacket EncodeThenDecode(MqttSubAckPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttSubAckCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.SubAck);
        consumed.ShouldBe(output.WrittenCount);

        return MqttSubAckCodec.Decode(body, packet.ProtocolVersion);
    }
}
