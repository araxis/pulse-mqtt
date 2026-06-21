using Microsoft.Extensions.Diagnostics.HealthChecks;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Reports a client's connection state: connected is healthy, transitional states are degraded,
/// and faulted/stopped/disconnected are unhealthy.
/// </summary>
public sealed class PulseMqttHealthCheck(ResilientMqttClient client) : IHealthCheck
{
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var snapshot = client.GetDiagnosticsSnapshot();
        var result = snapshot.State switch
        {
            ConnectionState.Connected => HealthCheckResult.Healthy(
                $"Connected (attempt {snapshot.Attempt}).",
                Data(snapshot)),
            ConnectionState.Connecting or ConnectionState.Reconnecting or ConnectionState.WaitingRetry =>
                HealthCheckResult.Degraded(
                    $"The connection is being established ({snapshot.State}, attempt {snapshot.Attempt}).",
                    data: Data(snapshot)),
            _ => HealthCheckResult.Unhealthy(
                UnhealthyDescription(snapshot),
                data: Data(snapshot)),
        };

        return Task.FromResult(result);
    }

    private static Dictionary<string, object> Data(MqttClientDiagnosticsSnapshot snapshot)
    {
        var data = new Dictionary<string, object>
        {
            ["client.id"] = snapshot.ClientId,
            ["state"] = snapshot.State.ToString(),
            ["attempt"] = snapshot.Attempt,
            ["is.running"] = snapshot.IsRunning,
            ["state.changed_at"] = snapshot.StateChangedAt,
            ["subscription.count"] = snapshot.SubscriptionCount,
            ["pending.subscribe.count"] = snapshot.PendingSubscribeCount,
            ["pending.unsubscribe.count"] = snapshot.PendingUnsubscribeCount,
        };

        if (snapshot.LastReason is { } reason)
        {
            data["reason"] = reason.ToString();
        }

        if (snapshot.LastReasonString is { } reasonString)
        {
            data["reason.string"] = reasonString;
        }

        if (snapshot.LastServerReference is { } serverReference)
        {
            data["server.reference"] = serverReference;
        }

        if (snapshot.LastError is { } error)
        {
            data["error.type"] = error.GetType().Name;
            data["error.message"] = error.Message;
        }

        if (snapshot.OfflineQueueDepth is { } depth)
        {
            data["offline.queue.depth"] = depth;
        }

        if (snapshot.OfflineQueueDroppedCount is { } dropped)
        {
            data["offline.queue.dropped"] = dropped;
        }

        return data;
    }

    private static string UnhealthyDescription(MqttClientDiagnosticsSnapshot snapshot)
    {
        if (snapshot.LastReason is { } reason)
        {
            return $"The client is {snapshot.State} ({reason}).";
        }

        return $"The client is {snapshot.State}.";
    }
}
