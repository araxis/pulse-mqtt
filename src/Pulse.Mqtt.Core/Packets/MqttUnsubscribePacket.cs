using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>An UNSUBSCRIBE control packet: the client's request to remove one or more subscriptions.</summary>
public sealed record MqttUnsubscribePacket : MqttPacket
{
    /// <summary>The packet identifier.</summary>
    public required ushort PacketIdentifier { get; init; }

    /// <summary>The topic filters to unsubscribe from. Must contain at least one.</summary>
    public required IReadOnlyList<string> TopicFilters { get; init; }

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>The MQTT 5 user properties carried on the UNSUBSCRIBE.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
