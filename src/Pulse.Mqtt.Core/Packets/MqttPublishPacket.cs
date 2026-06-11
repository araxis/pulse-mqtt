using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>A PUBLISH control packet carrying an application message.</summary>
public sealed record MqttPublishPacket : MqttPacket
{
    /// <summary>The topic the message is published to.</summary>
    public required string Topic { get; init; }

    /// <summary>The message payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>The delivery quality of service.</summary>
    public MqttQualityOfService QualityOfService { get; init; } = MqttQualityOfService.AtMostOnce;

    /// <summary>Whether this is a duplicate delivery of an earlier QoS &gt; 0 publish.</summary>
    public bool Dup { get; init; }

    /// <summary>Whether the broker should retain this message for the topic.</summary>
    public bool Retain { get; init; }

    /// <summary>The packet identifier. Present (and required) only when <see cref="QualityOfService"/> is greater than 0.</summary>
    public ushort? PacketIdentifier { get; init; }

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>The MQTT 5 payload format indicator.</summary>
    public MqttPayloadFormatIndicator PayloadFormatIndicator { get; init; } = MqttPayloadFormatIndicator.Unspecified;

    /// <summary>The MQTT 5 message expiry interval, in seconds, if set.</summary>
    public uint? MessageExpiryInterval { get; init; }

    /// <summary>The MQTT 5 topic alias, if set.</summary>
    public ushort? TopicAlias { get; init; }

    /// <summary>The MQTT 5 response topic, if set.</summary>
    public string? ResponseTopic { get; init; }

    /// <summary>The MQTT 5 correlation data, if set.</summary>
    public ReadOnlyMemory<byte>? CorrelationData { get; init; }

    /// <summary>The MQTT 5 content type, if set.</summary>
    public string? ContentType { get; init; }

    /// <summary>The MQTT 5 subscription identifiers that matched this message (inbound only; repeatable).</summary>
    public IReadOnlyList<uint> SubscriptionIdentifiers { get; init; } = [];

    /// <summary>The MQTT 5 user properties carried on the message.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
