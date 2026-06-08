namespace Pulse.Mqtt.Packets;

/// <summary>
/// The will message a broker publishes on the client's behalf if the connection closes
/// unexpectedly. Carried in the CONNECT packet payload.
/// </summary>
/// <param name="Topic">The topic the will message is published to.</param>
public sealed record MqttWillMessage(string Topic)
{
    /// <summary>The will payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>The QoS at which the will is published.</summary>
    public MqttQualityOfService QualityOfService { get; init; } = MqttQualityOfService.AtMostOnce;

    /// <summary>Whether the will is published as a retained message.</summary>
    public bool Retain { get; init; }

    /// <summary>The MQTT 5 will delay interval, in seconds, if set.</summary>
    public uint? DelayInterval { get; init; }

    /// <summary>The MQTT 5 payload format indicator.</summary>
    public MqttPayloadFormatIndicator PayloadFormatIndicator { get; init; } = MqttPayloadFormatIndicator.Unspecified;

    /// <summary>The MQTT 5 message expiry interval, in seconds, if set.</summary>
    public uint? MessageExpiryInterval { get; init; }

    /// <summary>The MQTT 5 content type, if set.</summary>
    public string? ContentType { get; init; }

    /// <summary>The MQTT 5 response topic, if set.</summary>
    public string? ResponseTopic { get; init; }

    /// <summary>The MQTT 5 correlation data, if set.</summary>
    public ReadOnlyMemory<byte>? CorrelationData { get; init; }

    /// <summary>The MQTT 5 user properties carried on the will.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
