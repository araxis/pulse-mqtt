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
}
