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
}
