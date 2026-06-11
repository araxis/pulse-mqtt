using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>An UNSUBACK control packet: the broker's response to an UNSUBSCRIBE.</summary>
public sealed record MqttUnsubAckPacket
{
    /// <summary>The packet identifier of the UNSUBSCRIBE being acknowledged.</summary>
    public required ushort PacketIdentifier { get; init; }

    /// <summary>
    /// One reason code per requested filter (MQTT 5 only). Empty for MQTT 3.1.1, which carries
    /// no payload on UNSUBACK.
    /// </summary>
    public required IReadOnlyList<MqttReasonCode> ReasonCodes { get; init; }

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>A human-readable reason string (MQTT 5), if present.</summary>
    public string? ReasonString { get; init; }

    /// <summary>The MQTT 5 user properties carried on the UNSUBACK.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
