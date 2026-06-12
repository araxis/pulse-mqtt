namespace Pulse.Mqtt.Connection;

/// <summary>Settings for a <see cref="RawMqttClient"/>.</summary>
public sealed record RawMqttClientOptions
{
    /// <summary>Engine settings (framing limits, inbound queue capacity).</summary>
    public MqttConnectionOptions Connection { get; init; } = new();

    /// <summary>How long to wait for the broker's CONNACK before the handshake fails.</summary>
    public TimeSpan ConnAckTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to wait for a PINGRESP after a keep-alive PINGREQ before faulting.</summary>
    public TimeSpan PingResponseTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to wait for the broker to acknowledge a publish, subscribe, or unsubscribe.</summary>
    public TimeSpan AcknowledgementTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The bounded capacity of the received-message queue.</summary>
    public int InboundMessageCapacity { get; init; } = 256;
}
