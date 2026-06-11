using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>A SUBACK control packet: the broker's response to a SUBSCRIBE, one reason code per filter.</summary>
public sealed record MqttSubAckPacket : MqttPacket
{
    /// <summary>The packet identifier of the SUBSCRIBE being acknowledged.</summary>
    public required ushort PacketIdentifier { get; init; }

    /// <summary>One reason code per requested filter — a granted QoS or a failure reason. Must contain at least one.</summary>
    public required IReadOnlyList<MqttReasonCode> ReasonCodes { get; init; }

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>A human-readable reason string (MQTT 5), if present.</summary>
    public string? ReasonString { get; init; }

    /// <summary>The MQTT 5 user properties carried on the SUBACK.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
