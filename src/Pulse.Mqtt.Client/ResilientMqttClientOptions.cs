using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Settings for a <see cref="ResilientMqttClient"/>. Every major behavior is a swap point: leave a
/// property unset to get the built-in default, or supply your own implementation (for example, a
/// Polly-backed reconnect strategy or a durable message store) without touching anything else.
/// </summary>
public sealed record ResilientMqttClientOptions
{
    /// <summary>The CONNECT packet template used for every (re)connection.</summary>
    public required MqttConnectPacket Connect { get; init; }

    /// <summary>Settings for the underlying per-connection client.</summary>
    public RawMqttClientOptions Raw { get; init; } = new();

    /// <summary>Bounds and overflow policy for the offline outbound queue.</summary>
    public OfflineQueueOptions OfflineQueue { get; init; } = new();

    /// <summary>Bounds for the default backoff strategy. Ignored when <see cref="ReconnectStrategy"/> is set.</summary>
    public BackoffOptions Backoff { get; init; } = new();

    /// <summary>Owns the reconnect loop. Default: exponential backoff with full jitter.</summary>
    public IReconnectStrategy? ReconnectStrategy { get; init; }

    /// <summary>Classifies connect failures as retryable or final. Default: auth/identity reasons are final.</summary>
    public IReconnectDecision? ReconnectDecision { get; init; }

    /// <summary>Runs on connection up/down. Default: re-subscribes from the session store when the session was lost.</summary>
    public IConnectionLifecycle? Lifecycle { get; init; }

    /// <summary>Holds the durable subscription set. Default: in-memory.</summary>
    public ISessionStore? SessionStore { get; init; }

    /// <summary>Holds publishes queued while offline. Default: bounded in-memory.</summary>
    public IMessageStore? MessageStore { get; init; }
}
