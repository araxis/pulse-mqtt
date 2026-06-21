namespace Pulse.Mqtt.Serialization.Protobuf;

/// <summary>
/// Maps generated Protocol Buffers message types to explicit parsers for reflection-free
/// deserialization.
/// </summary>
public sealed class ProtobufMessageRegistry
{
    private readonly IReadOnlyDictionary<Type, Func<ReadOnlyMemory<byte>, object>> _parsers;

    internal ProtobufMessageRegistry(IReadOnlyDictionary<Type, Func<ReadOnlyMemory<byte>, object>> parsers)
    {
        _parsers = new Dictionary<Type, Func<ReadOnlyMemory<byte>, object>>(parsers);
    }

    /// <summary>An empty registry. Serialization still works, but deserialization fails clearly.</summary>
    public static ProtobufMessageRegistry Empty { get; } = new(new Dictionary<Type, Func<ReadOnlyMemory<byte>, object>>());

    /// <summary>Creates an immutable registry from the configured generated-message parsers.</summary>
    public static ProtobufMessageRegistry Create(Action<ProtobufMessageRegistryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new ProtobufMessageRegistryBuilder();
        configure(builder);
        return builder.Build();
    }

    internal object Parse(Type type, ReadOnlyMemory<byte> payload)
    {
        if (!_parsers.TryGetValue(type, out var parser))
        {
            throw new MqttException($"No Protocol Buffers parser has been registered for {DisplayName(type)}.");
        }

        return parser(payload);
    }

    internal static string DisplayName(Type type) => type.FullName ?? type.Name;
}
