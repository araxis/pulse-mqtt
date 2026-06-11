using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttUnsubscribeCodecTests
{
    [Fact]
    public void V5_multi_filter_unsubscribe_round_trips()
    {
        var original = new MqttUnsubscribePacket
        {
            PacketIdentifier = 21,
            ProtocolVersion = MqttProtocolVersion.V500,
            TopicFilters = ["a/+", "b/#"],
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var decoded = EncodeThenDecode(original);

        decoded.PacketIdentifier.ShouldBe((ushort)21);
        decoded.TopicFilters.ShouldBe(new[] { "a/+", "b/#" });
        decoded.UserProperties.ShouldBe(original.UserProperties);
    }

    [Fact]
    public void V311_unsubscribe_round_trips()
    {
        var original = new MqttUnsubscribePacket
        {
            PacketIdentifier = 4,
            ProtocolVersion = MqttProtocolVersion.V311,
            TopicFilters = ["x/y"],
        };

        var decoded = EncodeThenDecode(original);

        decoded.TopicFilters.ShouldBe(new[] { "x/y" });
    }

    [Fact]
    public void Encode_rejects_empty_filter_list()
    {
        var packet = new MqttUnsubscribePacket { PacketIdentifier = 1, TopicFilters = [] };

        Should.Throw<ArgumentException>(() => MqttUnsubscribeCodec.Encode(new ArrayBufferWriter<byte>(), packet));
    }

    private static MqttUnsubscribePacket EncodeThenDecode(MqttUnsubscribePacket packet)
    {
        var output = new ArrayBufferWriter<byte>();
        MqttUnsubscribeCodec.Encode(output, packet);

        var status = MqttFrameReader.TryReadFrame(output.WrittenSpan, out var header, out var body, out var consumed);
        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Unsubscribe);
        header.Flags.ShouldBe((byte)0x02);
        consumed.ShouldBe(output.WrittenCount);

        return MqttUnsubscribeCodec.Decode(body, packet.ProtocolVersion);
    }
}
