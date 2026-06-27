using System.Buffers;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttProtocolCompatibilityTests
{
    [Fact]
    public void Publish_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttPublishPacket
        {
            Topic = "devices/1",
            ProtocolVersion = MqttProtocolVersion.V311,
            ContentType = "application/json",
        };

        var exception = Should.Throw<ArgumentException>(() => MqttPublishCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttPublishPacket.ContentType));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Connect_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttConnectPacket
        {
            ClientId = "client-1",
            ProtocolVersion = MqttProtocolVersion.V311,
            SessionExpiryInterval = 60,
        };

        var exception = Should.Throw<ArgumentException>(() => MqttConnectCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttConnectPacket.SessionExpiryInterval));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Connect_rejects_mqtt5_will_properties_when_encoded_as_v311()
    {
        var packet = new MqttConnectPacket
        {
            ClientId = "client-1",
            ProtocolVersion = MqttProtocolVersion.V311,
            Will = new MqttWillMessage("status/client-1")
            {
                ContentType = "text/plain",
            },
        };

        var exception = Should.Throw<ArgumentException>(() => MqttConnectCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttWillMessage.ContentType));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Subscribe_rejects_mqtt5_packet_properties_when_encoded_as_v311()
    {
        var packet = new MqttSubscribePacket
        {
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V311,
            SubscriptionIdentifier = 9,
            TopicFilters = [new MqttTopicFilter("devices/#")],
        };

        var exception = Should.Throw<ArgumentException>(() => MqttSubscribeCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttSubscribePacket.SubscriptionIdentifier));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Subscribe_rejects_mqtt5_filter_options_when_encoded_as_v311()
    {
        var packet = new MqttSubscribePacket
        {
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V311,
            TopicFilters = [new MqttTopicFilter("devices/#") { NoLocal = true }],
        };

        var exception = Should.Throw<ArgumentException>(() => MqttSubscribeCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttTopicFilter.NoLocal));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Unsubscribe_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttUnsubscribePacket
        {
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V311,
            TopicFilters = ["devices/#"],
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var exception = Should.Throw<ArgumentException>(() => MqttUnsubscribeCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttUnsubscribePacket.UserProperties));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Disconnect_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttDisconnectPacket
        {
            ProtocolVersion = MqttProtocolVersion.V311,
            ServerReference = "mqtt://alternate",
        };

        var exception = Should.Throw<ArgumentException>(() => MqttDisconnectCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttDisconnectPacket.ServerReference));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Publish_ack_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttPublishAckPacket
        {
            PacketType = MqttPacketType.PubAck,
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V311,
            ReasonString = "not accepted",
        };

        var exception = Should.Throw<ArgumentException>(() => MqttPublishAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttPublishAckPacket.ReasonString));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Connack_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttConnAckPacket
        {
            ProtocolVersion = MqttProtocolVersion.V311,
            ServerReference = "mqtt://alternate",
        };

        var exception = Should.Throw<ArgumentException>(() => MqttConnAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttConnAckPacket.ServerReference));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Suback_rejects_mqtt5_properties_when_encoded_as_v311()
    {
        var packet = new MqttSubAckPacket
        {
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V311,
            ReasonCodes = [MqttReasonCode.Success],
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        var exception = Should.Throw<ArgumentException>(() => MqttSubAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttSubAckPacket.UserProperties));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void Unsuback_rejects_mqtt5_reason_codes_when_encoded_as_v311()
    {
        var packet = new MqttUnsubAckPacket
        {
            PacketIdentifier = 1,
            ProtocolVersion = MqttProtocolVersion.V311,
            ReasonCodes = [MqttReasonCode.Success],
        };

        var exception = Should.Throw<ArgumentException>(() => MqttUnsubAckCodec.Encode(new ArrayBufferWriter<byte>(), packet));

        exception.Message.ShouldContain(nameof(MqttUnsubAckPacket.ReasonCodes));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }
}
