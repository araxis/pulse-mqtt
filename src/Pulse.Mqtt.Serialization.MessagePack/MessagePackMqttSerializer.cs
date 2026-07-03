using MessagePack;
using Pulse.Mqtt.Serialization;

namespace Pulse.Mqtt.Serialization.MessagePack;

/// <summary>
/// An <see cref="IMqttSerializer"/> over MessagePack — a compact binary format. Construction takes
/// the <see cref="MessagePackSerializerOptions"/> to use; supply options built from a
/// source-generated resolver (the MessagePack source generator, no dynamic codegen) to stay
/// trimming- and Native AOT-safe.
/// </summary>
public sealed class MessagePackMqttSerializer : IMqttSerializer
{
    private readonly MessagePackSerializerOptions _options;

    /// <summary>Creates a serializer over the given MessagePack options.</summary>
    public MessagePackMqttSerializer(MessagePackSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public string ContentType => "application/x-msgpack";

    /// <inheritdoc />
    public MqttPayloadFormatIndicator PayloadFormat => MqttPayloadFormatIndicator.Unspecified;

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize<T>(T value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return MessagePackSerializer.Serialize(value, _options);
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var result = MessagePackSerializer.Deserialize<T>(payload, _options);
            return result is T typed
                ? typed
                : throw new MqttException($"The payload did not deserialize to {typeof(T).Name}.");
        }
        catch (MessagePackSerializationException ex)
        {
            throw new MqttException($"The payload did not deserialize to {typeof(T).Name}.", ex);
        }
    }
}
