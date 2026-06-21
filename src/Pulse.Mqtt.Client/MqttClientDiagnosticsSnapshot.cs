using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Client;

/// <summary>A point-in-time, non-blocking diagnostics view of a resilient MQTT client.</summary>
public sealed record MqttClientDiagnosticsSnapshot(
    string ClientId,
    ConnectionState State,
    int Attempt,
    bool IsRunning,
    DateTimeOffset StateChangedAt,
    MqttReasonCode? LastReason,
    string? LastReasonString,
    string? LastServerReference,
    Exception? LastError,
    int? OfflineQueueDepth,
    long? OfflineQueueDroppedCount,
    int SubscriptionCount,
    int PendingSubscribeCount,
    int PendingUnsubscribeCount);
