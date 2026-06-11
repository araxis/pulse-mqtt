using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>A CONNACK control packet: the broker's response to a CONNECT.</summary>
public sealed record MqttConnAckPacket : MqttPacket
{
    /// <summary>Whether the broker already held session state for this client.</summary>
    public bool SessionPresent { get; init; }

    /// <summary>The connect result. <see cref="MqttReasonCode.Success"/> means the connection was accepted.</summary>
    public MqttReasonCode ReasonCode { get; init; } = MqttReasonCode.Success;

    /// <summary>The protocol version of the connection. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>The MQTT 5 session expiry interval the broker assigned, if present.</summary>
    public uint? SessionExpiryInterval { get; init; }

    /// <summary>The MQTT 5 receive maximum the broker will accept, if present.</summary>
    public ushort? ReceiveMaximum { get; init; }

    /// <summary>The MQTT 5 maximum QoS the broker supports, if present.</summary>
    public MqttQualityOfService? MaximumQoS { get; init; }

    /// <summary>Whether the broker supports retained messages, if present.</summary>
    public bool? RetainAvailable { get; init; }

    /// <summary>The MQTT 5 maximum packet size the broker will accept, if present.</summary>
    public uint? MaximumPacketSize { get; init; }

    /// <summary>The client identifier the broker assigned, if it generated one.</summary>
    public string? AssignedClientIdentifier { get; init; }

    /// <summary>The MQTT 5 topic alias maximum the broker supports, if present.</summary>
    public ushort? TopicAliasMaximum { get; init; }

    /// <summary>A human-readable reason string, if present.</summary>
    public string? ReasonString { get; init; }

    /// <summary>The MQTT 5 user properties carried on the CONNACK.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];

    /// <summary>Whether the broker supports wildcard subscriptions, if present.</summary>
    public bool? WildcardSubscriptionAvailable { get; init; }

    /// <summary>Whether the broker supports subscription identifiers, if present.</summary>
    public bool? SubscriptionIdentifiersAvailable { get; init; }

    /// <summary>Whether the broker supports shared subscriptions, if present.</summary>
    public bool? SharedSubscriptionAvailable { get; init; }

    /// <summary>The keep-alive the broker assigned, if it overrode the client's value.</summary>
    public ushort? ServerKeepAlive { get; init; }

    /// <summary>The MQTT 5 response information for request/response messaging, if present.</summary>
    public string? ResponseInformation { get; init; }

    /// <summary>An alternate server the client should use, if present.</summary>
    public string? ServerReference { get; init; }

    /// <summary>The MQTT 5 enhanced authentication method, if used.</summary>
    public string? AuthenticationMethod { get; init; }

    /// <summary>The MQTT 5 enhanced authentication data, if used.</summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; init; }
}
