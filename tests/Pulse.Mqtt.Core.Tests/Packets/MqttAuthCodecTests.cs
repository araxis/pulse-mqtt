using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttAuthCodecTests
{
    [Fact]
    public void Continue_authentication_with_properties_round_trips()
    {
        var original = new MqttAuthPacket
        {
            ReasonCode = MqttReasonCode.ContinueAuthentication,
            AuthenticationMethod = "SCRAM-SHA-1",
            AuthenticationData = new byte[] { 1, 2, 3, 4 },
            ReasonString = "step",
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var decoded = EncodeThenDecode(original);

        decoded.ReasonCode.ShouldBe(MqttReasonCode.ContinueAuthentication);
        decoded.AuthenticationMethod.ShouldBe("SCRAM-SHA-1");
        decoded.AuthenticationData!.Value.ToArray().ShouldBe(new byte[] { 1, 2, 3, 4 });
        decoded.ReasonString.ShouldBe("step");
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void Success_auth_is_empty()
    {
        var original = new MqttAuthPacket { ReasonCode = MqttReasonCode.Success };

        var (header, decoded) = EncodeThenDecode2(original);

        header.RemainingLength.ShouldBe(0);
        decoded.ReasonCode.ShouldBe(MqttReasonCode.Success);
        decoded.AuthenticationMethod.ShouldBeNull();
    }

    private static MqttAuthPacket EncodeThenDecode(MqttAuthPacket packet) => EncodeThenDecode2(packet).Packet;

    private static (MqttFixedHeader Header, MqttAuthPacket Packet) EncodeThenDecode2(MqttAuthPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttAuthCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Auth);
        consumed.ShouldBe(output.WrittenCount);

        return (header, MqttAuthCodec.Decode(body));
    }
}
