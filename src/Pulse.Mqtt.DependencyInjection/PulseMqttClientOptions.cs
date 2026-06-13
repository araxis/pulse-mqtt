using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Configuration for a named Pulse MQTT client — bindable from <c>appsettings.json</c>. Swappable
/// behaviors (transport, reconnect strategy, stores, serializer) are configured on the builder,
/// not here.
/// </summary>
public sealed class PulseMqttClientOptions
{
    /// <summary>The broker host name or IP address.</summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>The broker port. Conventionally 1883 for TCP and 8883 for TLS.</summary>
    public int Port { get; set; } = 1883;

    /// <summary>Whether to use TLS for the default TCP transport.</summary>
    public bool UseTls { get; set; }

    /// <summary>The MQTT client identifier.</summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>The keep-alive interval in seconds; 0 disables keep-alive.</summary>
    public ushort KeepAliveSeconds { get; set; } = 60;

    /// <summary>Whether to start a clean session.</summary>
    public bool CleanStart { get; set; } = true;

    /// <summary>The user name, when the broker requires authentication.</summary>
    public string? Username { get; set; }

    /// <summary>The password, when the broker requires authentication.</summary>
    public string? Password { get; set; }

    /// <summary>The MQTT protocol version. Defaults to 5.0.</summary>
    public MqttProtocolVersion ProtocolVersion { get; set; } = MqttProtocolVersion.V500;

    /// <summary>
    /// Whether the host starts the client on startup. Defaults to <see langword="true"/>. Set
    /// <see langword="false"/> to control the lifecycle yourself through
    /// <see cref="Client.ResilientMqttClient.StartAsync"/> and
    /// <see cref="Client.ResilientMqttClient.StopAsync"/> — start, stop, and restart at any
    /// time. Host shutdown still stops a running client either way.
    /// </summary>
    public bool StartWithHost { get; set; } = true;

    /// <summary>
    /// The last-will message the broker publishes when the connection dies ungracefully —
    /// bindable from configuration. Pair with <see cref="Birth"/> for the presence pattern.
    /// </summary>
    public PulseMqttWillOptions? Will { get; set; }

    /// <summary>The birth message published automatically on every connection-up — bindable from configuration.</summary>
    public PulseMqttBirthOptions? Birth { get; set; }
}

/// <summary>A configuration-bindable last-will message.</summary>
public sealed class PulseMqttWillOptions
{
    /// <summary>The topic the broker publishes the will to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>The UTF-8 will payload.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>The QoS the will is published at.</summary>
    public MqttQualityOfService QualityOfService { get; set; } = MqttQualityOfService.AtMostOnce;

    /// <summary>Whether the will is retained.</summary>
    public bool Retain { get; set; }

    /// <summary>The MQTT 5 will delay, in seconds, if set.</summary>
    public uint? DelaySeconds { get; set; }
}

/// <summary>A configuration-bindable birth message.</summary>
public sealed class PulseMqttBirthOptions
{
    /// <summary>The topic the birth message is published to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>The UTF-8 birth payload.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>The QoS the birth is published at.</summary>
    public MqttQualityOfService QualityOfService { get; set; } = MqttQualityOfService.AtMostOnce;

    /// <summary>Whether the birth is retained.</summary>
    public bool Retain { get; set; }
}
