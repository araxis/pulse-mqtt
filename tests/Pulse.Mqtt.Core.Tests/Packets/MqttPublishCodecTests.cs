using System.Buffers;
using System.Text;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttPublishCodecTests
{
    [Fact]
    public void Qos1_v5_publish_round_trips()
    {
        var original = new MqttPublishPacket
        {
            Topic = "sensors/1/temp",
            Payload = Encoding.UTF8.GetBytes("21.5"),
            QualityOfService = MqttQualityOfService.AtLeastOnce,
            Dup = false,
            Retain = true,
            PacketIdentifier = 4242,
            ProtocolVersion = MqttProtocolVersion.V500,
            PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8,
            MessageExpiryInterval = 60,
            ContentType = "text/plain",
            ResponseTopic = "responses/1",
            CorrelationData = new byte[] { 7, 8 },
            SubscriptionIdentifiers = [3, 9],
            UserProperties = [new MqttUserProperty("unit", "C")],
        };

        var decoded = EncodeThenDecode(original);

        decoded.Topic.ShouldBe("sensors/1/temp");
        Encoding.UTF8.GetString(decoded.Payload.Span).ShouldBe("21.5");
        decoded.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        decoded.Retain.ShouldBeTrue();
        decoded.Dup.ShouldBeFalse();
        decoded.PacketIdentifier.ShouldBe((ushort)4242);
        decoded.PayloadFormatIndicator.ShouldBe(MqttPayloadFormatIndicator.Utf8);
        decoded.MessageExpiryInterval.ShouldBe(60u);
        decoded.ContentType.ShouldBe("text/plain");
        decoded.ResponseTopic.ShouldBe("responses/1");
        decoded.CorrelationData!.Value.ToArray().ShouldBe(new byte[] { 7, 8 });
        decoded.SubscriptionIdentifiers.ShouldBe(new uint[] { 3, 9 });
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void Qos0_v5_publish_has_no_packet_identifier()
    {
        var original = new MqttPublishPacket
        {
            Topic = "events",
            Payload = Encoding.UTF8.GetBytes("hi"),
            QualityOfService = MqttQualityOfService.AtMostOnce,
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        var decoded = EncodeThenDecode(original);

        decoded.QualityOfService.ShouldBe(MqttQualityOfService.AtMostOnce);
        decoded.PacketIdentifier.ShouldBeNull();
        Encoding.UTF8.GetString(decoded.Payload.Span).ShouldBe("hi");
    }

    [Fact]
    public void V311_qos1_publish_round_trips()
    {
        var original = new MqttPublishPacket
        {
            Topic = "a/b",
            Payload = new byte[] { 1, 2, 3 },
            QualityOfService = MqttQualityOfService.AtLeastOnce,
            PacketIdentifier = 7,
            ProtocolVersion = MqttProtocolVersion.V311,
        };

        var decoded = EncodeThenDecode(original);

        decoded.Topic.ShouldBe("a/b");
        decoded.PacketIdentifier.ShouldBe((ushort)7);
        decoded.Payload.ToArray().ShouldBe(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void Decode_throws_on_qos3()
    {
        var header = new MqttFixedHeader(MqttPacketType.Publish, 0x06, 0); // QoS bits = 11

        Should.Throw<MqttProtocolException>(() => MqttPublishCodec.Decode(header, ReadOnlySpan<byte>.Empty, MqttProtocolVersion.V500));
    }

    [Fact]
    public void Encode_throws_when_qos1_without_packet_identifier()
    {
        var packet = new MqttPublishPacket
        {
            Topic = "t",
            QualityOfService = MqttQualityOfService.AtLeastOnce,
        };

        Should.Throw<ArgumentException>(() => MqttPublishCodec.Encode(new ArrayBufferWriter<byte>(), packet));
    }

    private static MqttPublishPacket EncodeThenDecode(MqttPublishPacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttPublishCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Publish);
        consumed.ShouldBe(output.WrittenCount);

        return MqttPublishCodec.Decode(header, body, packet.ProtocolVersion);
    }
}
