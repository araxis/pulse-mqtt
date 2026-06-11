using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>
/// A publish acknowledgement packet — PUBACK (QoS 1), or PUBREC / PUBREL / PUBCOMP (QoS 2).
/// All four share the same wire shape, distinguished by <see cref="PacketType"/>.
/// </summary>
public sealed record MqttPublishAckPacket : MqttPacket
{
    /// <summary>Which acknowledgement this is: PUBACK, PUBREC, PUBREL, or PUBCOMP.</summary>
    public required MqttPacketType PacketType { get; init; }

    /// <summary>The packet identifier being acknowledged.</summary>
    public required ushort PacketIdentifier { get; init; }

    /// <summary>The result. Defaults to <see cref="MqttReasonCode.Success"/>.</summary>
    public MqttReasonCode ReasonCode { get; init; } = MqttReasonCode.Success;

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>A human-readable reason string (MQTT 5), if present.</summary>
    public string? ReasonString { get; init; }

    /// <summary>The MQTT 5 user properties carried on the acknowledgement.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
