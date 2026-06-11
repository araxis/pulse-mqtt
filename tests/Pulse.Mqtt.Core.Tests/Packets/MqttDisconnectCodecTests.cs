using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttDisconnectCodecTests
{
    [Fact]
    public void V5_disconnect_with_reason_and_properties_round_trips()
    {
        var original = new MqttDisconnectPacket
        {
            ReasonCode = MqttReasonCode.ServerShuttingDown,
            ProtocolVersion = MqttProtocolVersion.V500,
            SessionExpiryInterval = 30,
            ReasonString = "bye",
            ServerReference = "other:1883",
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var decoded = EncodeThenDecode(original);

        decoded.ReasonCode.ShouldBe(MqttReasonCode.ServerShuttingDown);
        decoded.SessionExpiryInterval.ShouldBe(30u);
        decoded.ReasonString.ShouldBe("bye");
        decoded.ServerReference.ShouldBe("other:1883");
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void V5_normal_disconnect_is_empty()
    {
        var original = new MqttDisconnectPacket { ProtocolVersion = MqttProtocolVersion.V500 };

        var (header, decoded) = EncodeThenDecodeWithHeader(original);

        header.RemainingLength.ShouldBe(0);
        decoded.ReasonCode.ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public void V311_disconnect_is_empty()
    {
        var original = new MqttDisconnectPacket { ProtocolVersion = MqttProtocolVersion.V311 };

        var (header, decoded) = EncodeThenDecodeWithHeader(original);

        header.RemainingLength.ShouldBe(0);
        decoded.ReasonCode.ShouldBe(MqttReasonCode.Success);
    }

    private static MqttDisconnectPacket EncodeThenDecode(MqttDisconnectPacket packet) => EncodeThenDecodeWithHeader(packet).Packet;

    private static (MqttFixedHeader Header, MqttDisconnectPacket Packet) EncodeThenDecodeWithHeader(MqttDisconnectPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttDisconnectCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Disconnect);
        consumed.ShouldBe(output.WrittenCount);

        return (header, MqttDisconnectCodec.Decode(body, packet.ProtocolVersion));
    }
}
