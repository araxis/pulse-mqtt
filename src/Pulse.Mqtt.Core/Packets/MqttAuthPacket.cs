using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>An AUTH control packet for the MQTT 5 enhanced authentication exchange.</summary>
public sealed record MqttAuthPacket : MqttPacket
{
    /// <summary>
    /// The authentication reason: <see cref="MqttReasonCode.Success"/>,
    /// <see cref="MqttReasonCode.ContinueAuthentication"/>, or <see cref="MqttReasonCode.ReAuthenticate"/>.
    /// </summary>
    public MqttReasonCode ReasonCode { get; init; } = MqttReasonCode.Success;

    /// <summary>The authentication method, if present.</summary>
    public string? AuthenticationMethod { get; init; }

    /// <summary>The authentication data, if present.</summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; init; }

    /// <summary>A human-readable reason string, if present.</summary>
    public string? ReasonString { get; init; }

    /// <summary>The user properties carried on the AUTH.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
