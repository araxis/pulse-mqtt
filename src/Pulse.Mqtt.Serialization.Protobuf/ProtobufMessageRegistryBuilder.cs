using Google.Protobuf;

namespace Pulse.Mqtt.Serialization.Protobuf;

/// <summary>Builds a registry of generated Protocol Buffers message parsers.</summary>
public sealed class ProtobufMessageRegistryBuilder
{
    private readonly Dictionary<Type, Func<ReadOnlyMemory<byte>, object>> _parsers = [];

    /// <summary>Registers the parser for a generated message type.</summary>
    public ProtobufMessageRegistryBuilder Add<T>(MessageParser<T> parser)
        where T : class, IMessage<T>
    {
        ArgumentNullException.ThrowIfNull(parser);
        _parsers[typeof(T)] = payload => parser.ParseFrom(payload.Span);
        return this;
    }

    internal ProtobufMessageRegistry Build() => new(_parsers);
}
