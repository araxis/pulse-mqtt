using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Testing;

/// <summary>Options for the in-process MQTT test broker.</summary>
public sealed class PulseMqttTestBrokerOptions
{
    /// <summary>Whether retained publishes are stored and replayed to later matching subscriptions.</summary>
    public bool RetainedMessages { get; init; }

    /// <summary>Whether subscriptions are kept for reconnecting clients that use persistent sessions.</summary>
    public bool PersistentSessions { get; init; }

    /// <summary>The highest QoS the broker forwards to subscribers. Defaults to QoS 1.</summary>
    public MqttQualityOfService MaximumForwardQualityOfService { get; init; } = MqttQualityOfService.AtLeastOnce;

    /// <summary>
    /// Optional factory that can replace the broker's default CONNACK for a CONNECT packet.
    /// Return a non-success reason code to reject the connection.
    /// </summary>
    public Func<PulseMqttTestBrokerConnectContext, MqttConnAckPacket>? ConnAckFactory { get; init; }

    /// <summary>
    /// Optional factory that can replace the broker's default SUBACK for a SUBSCRIBE packet.
    /// Denied filters are not stored and do not receive retained-message replay.
    /// </summary>
    public Func<PulseMqttTestBrokerSubscribeContext, MqttSubAckPacket>? SubAckFactory { get; init; }

    /// <summary>
    /// Optional factory that can replace or withhold the broker's first publish acknowledgement
    /// for QoS 1/2 client publishes. Return <see langword="null"/> to simulate a timeout.
    /// </summary>
    public Func<PulseMqttTestBrokerPublishContext, MqttPublishAckPacket?>? PublishAckFactory { get; init; }
}
