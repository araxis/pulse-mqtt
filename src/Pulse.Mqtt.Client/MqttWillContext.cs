using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Client;

/// <summary>Connection-attempt context passed to an <see cref="IMqttWillProvider"/>.</summary>
public sealed class MqttWillContext
{
    /// <summary>Creates a context for one MQTT will generation call.</summary>
    public MqttWillContext(
        string clientId,
        int attempt,
        MqttProtocolVersion protocolVersion,
        bool cleanStart,
        ushort keepAliveSeconds,
        DateTimeOffset timestamp)
    {
        ArgumentNullException.ThrowIfNull(clientId);

        ClientId = clientId;
        Attempt = attempt;
        ProtocolVersion = protocolVersion;
        CleanStart = cleanStart;
        KeepAliveSeconds = keepAliveSeconds;
        Timestamp = timestamp;
    }

    /// <summary>The MQTT client identifier from the CONNECT packet template.</summary>
    public string ClientId { get; }

    /// <summary>The resilient-client connection attempt counter.</summary>
    public int Attempt { get; }

    /// <summary>The protocol version used by the CONNECT packet for this attempt.</summary>
    public MqttProtocolVersion ProtocolVersion { get; }

    /// <summary>Whether the CONNECT packet starts a clean session.</summary>
    public bool CleanStart { get; }

    /// <summary>The keep-alive value, in seconds, sent in the CONNECT packet.</summary>
    public ushort KeepAliveSeconds { get; }

    /// <summary>The UTC timestamp captured immediately before the will is requested.</summary>
    public DateTimeOffset Timestamp { get; }
}
