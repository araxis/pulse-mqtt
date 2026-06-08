using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttConnAckCodecTests
{
    [Fact]
    public void Full_v5_connack_round_trips()
    {
        var original = new MqttConnAckPacket
        {
            SessionPresent = true,
            ReasonCode = MqttReasonCode.Success,
            ProtocolVersion = MqttProtocolVersion.V500,
            SessionExpiryInterval = 300,
            ReceiveMaximum = 50,
            MaximumQoS = MqttQualityOfService.AtLeastOnce,
            RetainAvailable = true,
            MaximumPacketSize = 100_000,
            AssignedClientIdentifier = "auto-id",
            TopicAliasMaximum = 20,
            ReasonString = "ok",
            UserProperties = [new MqttUserProperty("k", "v")],
            WildcardSubscriptionAvailable = true,
            SubscriptionIdentifiersAvailable = false,
            SharedSubscriptionAvailable = true,
            ServerKeepAlive = 120,
            ResponseInformation = "resp",
            ServerReference = "ref",
            AuthenticationMethod = "SCRAM",
            AuthenticationData = new byte[] { 9, 9 },
        };

        var decoded = EncodeThenDecode(original);

        decoded.SessionPresent.ShouldBeTrue();
        decoded.ReasonCode.ShouldBe(MqttReasonCode.Success);
        decoded.SessionExpiryInterval.ShouldBe(300u);
        decoded.ReceiveMaximum.ShouldBe((ushort)50);
        decoded.MaximumQoS.ShouldBe(MqttQualityOfService.AtLeastOnce);
        decoded.RetainAvailable.ShouldBe(true);
        decoded.MaximumPacketSize.ShouldBe(100_000u);
        decoded.AssignedClientIdentifier.ShouldBe("auto-id");
        decoded.TopicAliasMaximum.ShouldBe((ushort)20);
        decoded.ReasonString.ShouldBe("ok");
        decoded.UserProperties.ShouldBe(original.UserProperties);
        decoded.WildcardSubscriptionAvailable.ShouldBe(true);
        decoded.SubscriptionIdentifiersAvailable.ShouldBe(false);
        decoded.SharedSubscriptionAvailable.ShouldBe(true);
        decoded.ServerKeepAlive.ShouldBe((ushort)120);
        decoded.ResponseInformation.ShouldBe("resp");
        decoded.ServerReference.ShouldBe("ref");
        decoded.AuthenticationMethod.ShouldBe("SCRAM");
        decoded.AuthenticationData!.Value.ToArray().ShouldBe(new byte[] { 9, 9 });
    }

    [Fact]
    public void V311_connack_round_trips()
    {
        var original = new MqttConnAckPacket
        {
            SessionPresent = false,
            ReasonCode = MqttReasonCode.Success,
            ProtocolVersion = MqttProtocolVersion.V311,
        };

        var decoded = EncodeThenDecode(original);

        decoded.SessionPresent.ShouldBeFalse();
        decoded.ReasonCode.ShouldBe(MqttReasonCode.Success);
        decoded.SessionExpiryInterval.ShouldBeNull();
        decoded.AuthenticationData.ShouldBeNull();
    }

    [Fact]
    public void Decode_throws_when_reserved_ack_flag_set()
    {
        byte[] body = [0x02, 0x00]; // reserved bit set in the acknowledge flags

        Should.Throw<MqttProtocolException>(() => MqttConnAckCodec.Decode(body, MqttProtocolVersion.V500));
    }

    private static MqttConnAckPacket EncodeThenDecode(MqttConnAckPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttConnAckCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.ConnAck);
        consumed.ShouldBe(output.WrittenCount);

        return MqttConnAckCodec.Decode(body, packet.ProtocolVersion);
    }
}
