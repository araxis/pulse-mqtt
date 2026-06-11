using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>A DISCONNECT control packet, sent by either the client or the broker to close the connection.</summary>
public sealed record MqttDisconnectPacket
{
    /// <summary>The disconnect reason. Defaults to <see cref="MqttReasonCode.Success"/> (normal disconnection).</summary>
    public MqttReasonCode ReasonCode { get; init; } = MqttReasonCode.Success;

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>The MQTT 5 session expiry interval to apply on disconnect, if set.</summary>
    public uint? SessionExpiryInterval { get; init; }

    /// <summary>A human-readable reason string (MQTT 5), if present.</summary>
    public string? ReasonString { get; init; }

    /// <summary>An alternate server the client should use (MQTT 5), if present.</summary>
    public string? ServerReference { get; init; }

    /// <summary>The MQTT 5 user properties carried on the DISCONNECT.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
