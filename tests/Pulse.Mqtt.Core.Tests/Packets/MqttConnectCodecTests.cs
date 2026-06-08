using System.Buffers;
using System.Text;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttConnectCodecTests
{
    [Fact]
    public void Full_v5_connect_round_trips()
    {
        var original = new MqttConnectPacket
        {
            ClientId = "client-1",
            ProtocolVersion = MqttProtocolVersion.V500,
            CleanStart = true,
            KeepAliveSeconds = 30,
            Username = "user",
            Password = Encoding.UTF8.GetBytes("secret"),
            SessionExpiryInterval = 120,
            ReceiveMaximum = 100,
            MaximumPacketSize = 65_535,
            TopicAliasMaximum = 10,
            RequestResponseInformation = true,
            RequestProblemInformation = false,
            AuthenticationMethod = "SCRAM-SHA-1",
            AuthenticationData = new byte[] { 1, 2, 3 },
            UserProperties = [new MqttUserProperty("a", "b"), new MqttUserProperty("c", "d")],
            Will = new MqttWillMessage("will/topic")
            {
                Payload = Encoding.UTF8.GetBytes("bye"),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                Retain = true,
                DelayInterval = 5,
                ContentType = "text/plain",
                UserProperties = [new MqttUserProperty("w", "x")],
            },
        };

        var decoded = EncodeThenDecode(original);

        decoded.ClientId.ShouldBe("client-1");
        decoded.ProtocolVersion.ShouldBe(MqttProtocolVersion.V500);
        decoded.CleanStart.ShouldBeTrue();
        decoded.KeepAliveSeconds.ShouldBe((ushort)30);
        decoded.Username.ShouldBe("user");
        Encoding.UTF8.GetString(decoded.Password!.Value.Span).ShouldBe("secret");
        decoded.SessionExpiryInterval.ShouldBe(120u);
        decoded.ReceiveMaximum.ShouldBe((ushort)100);
        decoded.MaximumPacketSize.ShouldBe(65_535u);
        decoded.TopicAliasMaximum.ShouldBe((ushort)10);
        decoded.RequestResponseInformation.ShouldBeTrue();
        decoded.RequestProblemInformation.ShouldBeFalse();
        decoded.AuthenticationMethod.ShouldBe("SCRAM-SHA-1");
        decoded.AuthenticationData!.Value.ToArray().ShouldBe(new byte[] { 1, 2, 3 });
        decoded.UserProperties.ShouldBe(original.UserProperties);

        decoded.Will.ShouldNotBeNull();
        decoded.Will!.Topic.ShouldBe("will/topic");
        Encoding.UTF8.GetString(decoded.Will.Payload.Span).ShouldBe("bye");
        decoded.Will.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        decoded.Will.Retain.ShouldBeTrue();
        decoded.Will.DelayInterval.ShouldBe(5u);
        decoded.Will.ContentType.ShouldBe("text/plain");
        decoded.Will.UserProperties.ShouldBe(original.Will.UserProperties);
    }

    [Fact]
    public void Minimal_v311_connect_round_trips()
    {
        var original = new MqttConnectPacket
        {
            ClientId = "c",
            ProtocolVersion = MqttProtocolVersion.V311,
            CleanStart = true,
            KeepAliveSeconds = 60,
        };

        var decoded = EncodeThenDecode(original);

        decoded.ClientId.ShouldBe("c");
        decoded.ProtocolVersion.ShouldBe(MqttProtocolVersion.V311);
        decoded.CleanStart.ShouldBeTrue();
        decoded.KeepAliveSeconds.ShouldBe((ushort)60);
        decoded.Will.ShouldBeNull();
        decoded.Username.ShouldBeNull();
        decoded.Password.ShouldBeNull();
    }

    [Fact]
    public void Decode_throws_on_wrong_protocol_name()
    {
        byte[] body = [0x00, 0x03, (byte)'X', (byte)'Y', (byte)'Z'];

        Should.Throw<MqttProtocolException>(() => MqttConnectCodec.Decode(body));
    }

    [Fact]
    public void Decode_throws_when_reserved_flag_set()
    {
        // "MQTT" + level 5 + flags 0x01 (reserved bit set).
        byte[] body = [0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T', 0x05, 0x01];

        Should.Throw<MqttProtocolException>(() => MqttConnectCodec.Decode(body));
    }

    private static MqttConnectPacket EncodeThenDecode(MqttConnectPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttConnectCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Connect);
        consumed.ShouldBe(output.WrittenCount);

        return MqttConnectCodec.Decode(body);
    }
}
