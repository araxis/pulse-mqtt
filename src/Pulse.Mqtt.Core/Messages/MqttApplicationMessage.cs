namespace Pulse.Mqtt;

/// <summary>
/// An application message to publish or one received from the broker. Carries the topic, payload,
/// quality of service, and retain flag, plus the optional MQTT 5 metadata.
/// </summary>
/// <param name="Topic">The topic the message is published to.</param>
public sealed record MqttApplicationMessage(string Topic)
{
    /// <summary>The message payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>The delivery guarantee. Defaults to <see cref="MqttQualityOfService.AtMostOnce"/>.</summary>
    public MqttQualityOfService QualityOfService { get; init; } = MqttQualityOfService.AtMostOnce;

    /// <summary>Whether the broker should retain this message for the topic.</summary>
    public bool Retain { get; init; }

    /// <summary>The MQTT 5 payload format indicator.</summary>
    public MqttPayloadFormatIndicator PayloadFormatIndicator { get; init; } = MqttPayloadFormatIndicator.Unspecified;

    /// <summary>The MQTT 5 message expiry interval, in seconds, if set.</summary>
    public uint? MessageExpiryInterval { get; init; }

    /// <summary>The MQTT 5 content type (a MIME-style descriptor), if set.</summary>
    public string? ContentType { get; init; }

    /// <summary>The MQTT 5 response topic for request/response messaging, if set.</summary>
    public string? ResponseTopic { get; init; }

    /// <summary>The MQTT 5 correlation data for request/response messaging, if set.</summary>
    public ReadOnlyMemory<byte>? CorrelationData { get; init; }

    /// <summary>The MQTT 5 topic alias, if set.</summary>
    public ushort? TopicAlias { get; init; }

    /// <summary>The MQTT 5 subscription identifiers that matched this message (inbound only).</summary>
    public IReadOnlyList<uint> SubscriptionIdentifiers { get; init; } = [];

    /// <summary>The MQTT 5 user properties carried on the message.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
