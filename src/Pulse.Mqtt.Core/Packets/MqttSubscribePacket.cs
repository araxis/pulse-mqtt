using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>A SUBSCRIBE control packet: the client's request to subscribe to one or more topic filters.</summary>
public sealed record MqttSubscribePacket : MqttPacket
{
    /// <summary>The packet identifier.</summary>
    public required ushort PacketIdentifier { get; init; }

    /// <summary>The topic filters to subscribe to, with their per-filter options. Must contain at least one.</summary>
    public required IReadOnlyList<MqttTopicFilter> TopicFilters { get; init; }

    /// <summary>The protocol version. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>The MQTT 5 subscription identifier applied to every filter in this request, if set.</summary>
    public uint? SubscriptionIdentifier { get; init; }

    /// <summary>The MQTT 5 user properties carried on the SUBSCRIBE.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
