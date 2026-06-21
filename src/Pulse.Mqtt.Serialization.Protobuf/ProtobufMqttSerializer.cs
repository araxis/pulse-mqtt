using Google.Protobuf;
using Pulse.Mqtt.Serialization;

namespace Pulse.Mqtt.Serialization.Protobuf;

/// <summary>
/// An <see cref="IMqttSerializer"/> over generated Protocol Buffers messages. Deserialization uses
/// explicit parser registration, avoiding reflection-based parser discovery for trimming and Native
/// AOT.
/// </summary>
public sealed class ProtobufMqttSerializer : IMqttSerializer
{
    private readonly ProtobufMessageRegistry _registry;
    private readonly string _contentType;
    private readonly MqttPayloadFormatIndicator _payloadFormat;

    /// <summary>Creates a serializer over the given parser registry.</summary>
    public ProtobufMqttSerializer(ProtobufMessageRegistry registry)
        : this(new ProtobufMqttSerializerOptions { Registry = registry })
    {
    }

    /// <summary>Creates a serializer over the given options.</summary>
    public ProtobufMqttSerializer(ProtobufMqttSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Registry);

        if (string.IsNullOrWhiteSpace(options.ContentType))
        {
            throw new ArgumentException("ContentType must not be empty.", nameof(options));
        }

        _registry = options.Registry;
        _contentType = options.ContentType;
        _payloadFormat = options.PayloadFormat;
    }

    /// <inheritdoc />
    public string ContentType => _contentType;

    /// <inheritdoc />
    public MqttPayloadFormatIndicator PayloadFormat => _payloadFormat;

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize<T>(T value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value is not IMessage message)
        {
            throw new MqttException($"The value of type {ProtobufMessageRegistry.DisplayName(typeof(T))} is not a generated Protocol Buffers message.");
        }

        return message.ToByteArray();
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var result = _registry.Parse(typeof(T), payload);
            return result is T typed
                ? typed
                : throw new MqttException($"The payload did not deserialize to {ProtobufMessageRegistry.DisplayName(typeof(T))}.");
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new MqttException($"The payload did not deserialize to {ProtobufMessageRegistry.DisplayName(typeof(T))}.", ex);
        }
    }
}
