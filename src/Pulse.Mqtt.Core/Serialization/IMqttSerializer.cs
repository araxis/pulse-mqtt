namespace Pulse.Mqtt.Serialization;

/// <summary>
/// Converts typed payloads to and from bytes. This is a swap point: JSON, MessagePack, Protobuf,
/// or custom formats plug in here, and the client stamps each typed publish with the serializer's
/// content type and payload format so the wire metadata stays honest.
/// </summary>
public interface IMqttSerializer
{
    /// <summary>The MQTT 5 content type stamped on typed publishes (for example <c>application/json</c>).</summary>
    string ContentType { get; }

    /// <summary>The MQTT 5 payload format indicator stamped on typed publishes.</summary>
    MqttPayloadFormatIndicator PayloadFormat { get; }

    /// <summary>Serializes <paramref name="value"/> to a payload.</summary>
    ReadOnlyMemory<byte> Serialize<T>(T value);

    /// <summary>Deserializes a payload to <typeparamref name="T"/>.</summary>
    /// <exception cref="MqttException">The payload does not represent a <typeparamref name="T"/>.</exception>
    T Deserialize<T>(ReadOnlyMemory<byte> payload);
}
