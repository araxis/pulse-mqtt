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

    /// <summary>
    /// Whether to compress repeated publish topics into MQTT 5 topic aliases, within the
    /// maximum the broker advertises. Aliases are assigned first come, first served, and reset
    /// on every connection. Off by default.
    /// </summary>
    public bool UseOutboundTopicAliases { get; init; }

    /// <summary>
    /// The MQTT 5 enhanced-authentication handler. When set, its method and initial data ride
    /// the CONNECT, broker AUTH challenges are answered through it during the handshake, and
    /// <see cref="RawMqttClient.ReAuthenticateAsync"/> becomes available. <see langword="null"/>
    /// (the default) sends no AUTH and costs nothing.
    /// </summary>
    public IMqttAuthenticator? Authenticator { get; init; }
}
