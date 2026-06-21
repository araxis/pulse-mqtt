namespace Pulse.Mqtt.Serialization.Protobuf;

/// <summary>Options for <see cref="ProtobufMqttSerializer"/>.</summary>
public sealed class ProtobufMqttSerializerOptions
{
    /// <summary>The parser registry used for deserialization.</summary>
    public ProtobufMessageRegistry Registry { get; init; } = ProtobufMessageRegistry.Empty;

    /// <summary>The MQTT 5 content type stamped on typed publishes.</summary>
    public string ContentType { get; init; } = "application/x-protobuf";

    /// <summary>The MQTT 5 payload format indicator stamped on typed publishes.</summary>
    public MqttPayloadFormatIndicator PayloadFormat { get; init; } = MqttPayloadFormatIndicator.Unspecified;
}
