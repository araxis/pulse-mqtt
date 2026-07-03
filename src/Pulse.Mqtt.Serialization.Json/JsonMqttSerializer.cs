using System.Text.Json;
using System.Text.Json.Serialization;
using Pulse.Mqtt.Serialization;

namespace Pulse.Mqtt.Serialization.Json;

/// <summary>
/// An <see cref="IMqttSerializer"/> over <c>System.Text.Json</c> source generation. Construction
/// requires a <see cref="JsonSerializerContext"/>, so serialization stays reflection-free and
/// safe for trimming and Native AOT.
/// </summary>
public sealed class JsonMqttSerializer : IMqttSerializer
{
    private readonly JsonSerializerContext _context;

    /// <summary>Creates a serializer over the given source-generated context.</summary>
    public JsonMqttSerializer(JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public MqttPayloadFormatIndicator PayloadFormat => MqttPayloadFormatIndicator.Utf8;

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize<T>(T value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return JsonSerializer.SerializeToUtf8Bytes(value, typeof(T), _context);
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var result = JsonSerializer.Deserialize(payload.Span, typeof(T), _context);
            return result is T typed
                ? typed
                : throw new MqttException($"The payload did not deserialize to {typeof(T).Name}.");
        }
        catch (JsonException ex)
        {
            // An empty or malformed payload (e.g. a zero-byte retained-message clear) makes
            // System.Text.Json throw JsonException. Surface it as the MqttException the
            // IMqttSerializer contract documents, matching the MessagePack and Protobuf serializers.
            throw new MqttException($"The payload did not deserialize to {typeof(T).Name}.", ex);
        }
    }
}
