using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttSubscribeCodecTests
{
    [Fact]
    public void V5_multi_filter_subscribe_round_trips()
    {
        var original = new MqttSubscribePacket
        {
            PacketIdentifier = 11,
            ProtocolVersion = MqttProtocolVersion.V500,
            SubscriptionIdentifier = 42,
            UserProperties = [new MqttUserProperty("k", "v")],
            TopicFilters =
            [
                new MqttTopicFilter("a/+") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce, NoLocal = true },
                new MqttTopicFilter("b/#") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce, RetainHandling = MqttRetainHandling.DoNotSendAtSubscribe },
            ],
        };

        var decoded = EncodeThenDecode(original);

        decoded.PacketIdentifier.ShouldBe((ushort)11);
        decoded.SubscriptionIdentifier.ShouldBe(42u);
        decoded.UserProperties.ShouldBe(original.UserProperties);
        decoded.TopicFilters.Count.ShouldBe(2);
        decoded.TopicFilters[0].Topic.ShouldBe("a/+");
        decoded.TopicFilters[0].MaximumQualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        decoded.TopicFilters[0].NoLocal.ShouldBeTrue();
        decoded.TopicFilters[1].Topic.ShouldBe("b/#");
        decoded.TopicFilters[1].RetainHandling.ShouldBe(MqttRetainHandling.DoNotSendAtSubscribe);
    }

    [Fact]
    public void V311_subscribe_round_trips_qos_only()
    {
        var original = new MqttSubscribePacket
        {
            PacketIdentifier = 5,
            ProtocolVersion = MqttProtocolVersion.V311,
            TopicFilters = [new MqttTopicFilter("x") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
        };

        var decoded = EncodeThenDecode(original);

        decoded.TopicFilters.Count.ShouldBe(1);
        decoded.TopicFilters[0].Topic.ShouldBe("x");
        decoded.TopicFilters[0].MaximumQualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
    }

    [Fact]
    public void Encode_rejects_empty_filter_list()
    {
        var packet = new MqttSubscribePacket { PacketIdentifier = 1, TopicFilters = [] };

        Should.Throw<ArgumentException>(() => MqttSubscribeCodec.Encode(new ArrayBufferWriter<byte>(), packet));
    }

    private static MqttSubscribePacket EncodeThenDecode(MqttSubscribePacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttSubscribeCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Subscribe);
        header.Flags.ShouldBe((byte)0x02);
        consumed.ShouldBe(output.WrittenCount);

        return MqttSubscribeCodec.Decode(body, packet.ProtocolVersion);
    }
}
