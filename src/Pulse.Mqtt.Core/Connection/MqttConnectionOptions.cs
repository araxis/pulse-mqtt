using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Connection;

/// <summary>Settings for an <see cref="MqttConnection"/>.</summary>
public sealed record MqttConnectionOptions
{
    /// <summary>The negotiated protocol version used to decode inbound packets.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>
    /// The largest inbound packet accepted, in bytes (fixed header included). Larger packets fault
    /// the connection with a protocol error. Defaults to the protocol maximum.
    /// </summary>
    public int MaxInboundPacketSize { get; init; } = MqttVarInt.MaxValue;

    /// <summary>
    /// The bounded capacity of the inbound packet queue. When the consumer falls behind, the
    /// receive loop waits — backpressure flows to the transport instead of buffering unboundedly.
    /// </summary>
    public int InboundQueueCapacity { get; init; } = 256;
}
