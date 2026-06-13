using Microsoft.Extensions.Logging;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Serialization;

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

    /// <summary>
    /// Converts typed payloads for <c>PublishAsync&lt;T&gt;</c> / <c>OnAsync&lt;T&gt;</c>. No default —
    /// typed messaging throws until one is configured (for example the JSON serializer add-on).
    /// </summary>
    public IMqttSerializer? Serializer { get; init; }

    /// <summary>Receives the client's structured logs. <see langword="null"/> (the default) is silent.</summary>
    public ILogger? Logger { get; init; }

    /// <summary>
    /// When enabled, the active span's W3C trace context (<c>traceparent</c>/<c>tracestate</c>) is
    /// written onto the user properties of outbound publishes, so a consumer's receive span links
    /// to the producer's span across processes. Off by default; receive spans always honor an
    /// incoming <c>traceparent</c> regardless of this flag.
    /// </summary>
    public bool PropagateTraceContext { get; init; }

    /// <summary>
    /// The last-will message registered with every CONNECT — the broker publishes it when the
    /// connection dies ungracefully. Overrides <see cref="MqttConnectPacket.Will"/> on
    /// <see cref="Connect"/> when set; <see cref="WillFactory"/> wins over both.
    /// </summary>
    public MqttWillMessage? Will { get; init; }

    /// <summary>
    /// Computes the will fresh for every connection attempt — timestamps, session counters,
    /// current configuration — instead of freezing it at startup. A throwing factory fails
    /// that attempt like any connect failure, classified by the reconnect decision.
    /// </summary>
    public Func<CancellationToken, ValueTask<MqttWillMessage>>? WillFactory { get; init; }

    /// <summary>
    /// The birth message — the will's mirror — published automatically on every connection-up:
    /// after re-subscription, before the offline queue flushes, before the state becomes
    /// <see cref="Resilience.ConnectionState.Connected"/>. Pair a retained birth ("online")
    /// with a retained will ("offline") for the classic presence pattern.
    /// </summary>
    public MqttPublishPacket? Birth { get; init; }

    /// <summary>
    /// Computes the birth message fresh for every connection-up; the argument is the
    /// connection attempt counter. Wins over <see cref="Birth"/> when both are set.
    /// </summary>
    public Func<int, CancellationToken, ValueTask<MqttPublishPacket>>? BirthFactory { get; init; }

    /// <summary>What happens when publishing the birth message fails. Default: the connection-up fails and retries.</summary>
    public BirthFailurePolicy BirthFailure { get; init; } = BirthFailurePolicy.FailConnection;
}

/// <summary>How a failed birth publish is treated.</summary>
public enum BirthFailurePolicy
{
    /// <summary>
    /// The connection-up fails and the reconnect cycle retries — presence stays truthful:
    /// nobody observes a connected client whose "online" announcement never went out.
    /// </summary>
    FailConnection,

    /// <summary>The failure is logged and counted; the connection proceeds without the announcement.</summary>
    LogAndContinue,
}
