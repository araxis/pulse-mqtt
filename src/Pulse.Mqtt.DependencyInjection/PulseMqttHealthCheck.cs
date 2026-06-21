using Microsoft.Extensions.Diagnostics.HealthChecks;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Reports a client's connection state: connected is healthy, transitional states are degraded,
/// and faulted/stopped/disconnected are unhealthy.
/// </summary>
public sealed class PulseMqttHealthCheck : IHealthCheck
{
    private readonly ResilientMqttClient _client;
    private readonly PulseMqttHealthCheckOptions _options;

    /// <summary>Creates a health check with the default connection-state mapping.</summary>
    public PulseMqttHealthCheck(ResilientMqttClient client)
        : this(client, options: null)
    {
    }

    /// <summary>Creates a health check with optional policy thresholds.</summary>
    public PulseMqttHealthCheck(ResilientMqttClient client, PulseMqttHealthCheckOptions? options)
    {
        ArgumentNullException.ThrowIfNull(client);

        _client = client;
        _options = (options ?? new PulseMqttHealthCheckOptions()).Snapshot();
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var snapshot = _client.GetDiagnosticsSnapshot();
        var data = Data(snapshot);
        var result = snapshot.State switch
        {
            ConnectionState.Connected => new HealthCheckResult(
                HealthStatus.Healthy,
                $"Connected (attempt {snapshot.Attempt}).",
                data: data),
            ConnectionState.Connecting or ConnectionState.Reconnecting or ConnectionState.WaitingRetry =>
                new HealthCheckResult(
                    HealthStatus.Degraded,
                    $"The connection is being established ({snapshot.State}, attempt {snapshot.Attempt}).",
                    data: data),
            _ => new HealthCheckResult(
                HealthStatus.Unhealthy,
                UnhealthyDescription(snapshot),
                data: data),
        };

        var policy = EvaluatePolicy(snapshot);
        if (policy is not null && IsWorse(policy.Value.Status, result.Status))
        {
            data["health.policy.status"] = policy.Value.Status.ToString();
            data["health.policy.reasons"] = policy.Value.Reasons;
            result = new HealthCheckResult(
                policy.Value.Status,
                PolicyDescription(policy.Value),
                data: data);
        }

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

        if (snapshot.BrokerCapabilities is { } capabilities)
        {
            AddBrokerCapabilities(data, capabilities);
        }

        return data;
    }

    private PolicyResult? EvaluatePolicy(MqttClientDiagnosticsSnapshot snapshot)
    {
        List<string> unhealthyReasons = [];
        List<string> degradedReasons = [];

        if (snapshot.OfflineQueueDepth is { } depth)
        {
            AddThresholdReason(
                unhealthyReasons,
                depth,
                _options.UnhealthyOfflineQueueDepthThreshold,
                "offline.queue.depth");
            AddThresholdReason(
                degradedReasons,
                depth,
                _options.DegradedOfflineQueueDepthThreshold,
                "offline.queue.depth");
        }

        if (snapshot.OfflineQueueDroppedCount is { } dropped)
        {
            AddThresholdReason(
                unhealthyReasons,
                dropped,
                _options.UnhealthyOfflineQueueDroppedCountThreshold,
                "offline.queue.dropped");
            AddThresholdReason(
                degradedReasons,
                dropped,
                _options.DegradedOfflineQueueDroppedCountThreshold,
                "offline.queue.dropped");
        }

        var pendingOperations = snapshot.PendingSubscribeCount + snapshot.PendingUnsubscribeCount;
        AddThresholdReason(
            unhealthyReasons,
            pendingOperations,
            _options.UnhealthyPendingSubscriptionOperationsThreshold,
            "pending.subscription.operations");
        AddThresholdReason(
            degradedReasons,
            pendingOperations,
            _options.DegradedPendingSubscriptionOperationsThreshold,
            "pending.subscription.operations");

        if (unhealthyReasons.Count > 0)
        {
            return new PolicyResult(HealthStatus.Unhealthy, [.. unhealthyReasons]);
        }

        return degradedReasons.Count > 0
            ? new PolicyResult(HealthStatus.Degraded, [.. degradedReasons])
            : null;
    }

    private static void AddThresholdReason(List<string> reasons, int value, int? threshold, string key)
    {
        if (threshold is { } setThreshold && value >= setThreshold)
        {
            reasons.Add($"{key} {value} >= {setThreshold}");
        }
    }

    private static void AddThresholdReason(List<string> reasons, long value, long? threshold, string key)
    {
        if (threshold is { } setThreshold && value >= setThreshold)
        {
            reasons.Add($"{key} {value} >= {setThreshold}");
        }
    }

    private static bool IsWorse(HealthStatus candidate, HealthStatus current) =>
        Severity(candidate) > Severity(current);

    private static int Severity(HealthStatus status) =>
        status switch
        {
            HealthStatus.Healthy => 0,
            HealthStatus.Degraded => 1,
            _ => 2,
        };

    private static string PolicyDescription(PolicyResult policy) =>
        $"Health policy marked the client {policy.Status.ToString().ToLowerInvariant()}: {string.Join("; ", policy.Reasons)}.";

    private static void AddBrokerCapabilities(
        Dictionary<string, object> data,
        MqttBrokerCapabilitiesSnapshot capabilities)
    {
        data["broker.protocol.version"] = capabilities.ProtocolVersion.ToString();
        data["broker.session.present"] = capabilities.SessionPresent;
        data["broker.maximum.qos.effective"] = capabilities.EffectiveMaximumQoS.ToString();
        data["broker.retained.messages"] = capabilities.RetainedMessages.ToString();
        data["broker.topic.alias.maximum.effective"] = capabilities.EffectiveTopicAliasMaximum;
        data["broker.topic.aliases"] = capabilities.TopicAliases.ToString();
        data["broker.wildcard.subscriptions"] = capabilities.WildcardSubscriptions.ToString();
        data["broker.subscription.identifiers"] = capabilities.SubscriptionIdentifiers.ToString();
        data["broker.shared.subscriptions"] = capabilities.SharedSubscriptions.ToString();
        data["broker.keep_alive.effective"] = capabilities.EffectiveKeepAliveSeconds;

        if (capabilities.EffectiveReceiveMaximum is { } effectiveReceiveMaximum)
        {
            data["broker.receive.maximum.effective"] = effectiveReceiveMaximum;
        }

        if (capabilities.AssignedClientIdentifier is { } assignedClientIdentifier)
        {
            data["broker.assigned.client.id"] = assignedClientIdentifier;
        }

        if (capabilities.ReceiveMaximum is { } receiveMaximum)
        {
            data["broker.receive.maximum"] = receiveMaximum;
        }

        if (capabilities.MaximumQoS is { } maximumQoS)
        {
            data["broker.maximum.qos"] = maximumQoS.ToString();
        }

        if (capabilities.RetainAvailable is { } retainAvailable)
        {
            data["broker.retain.available"] = retainAvailable;
        }

        if (capabilities.MaximumPacketSize is { } maximumPacketSize)
        {
            data["broker.maximum.packet.size"] = maximumPacketSize;
        }

        if (capabilities.TopicAliasMaximum is { } topicAliasMaximum)
        {
            data["broker.topic.alias.maximum"] = topicAliasMaximum;
        }

        if (capabilities.WildcardSubscriptionAvailable is { } wildcardSubscriptionAvailable)
        {
            data["broker.wildcard.subscription.available"] = wildcardSubscriptionAvailable;
        }

        if (capabilities.SubscriptionIdentifiersAvailable is { } subscriptionIdentifiersAvailable)
        {
            data["broker.subscription.identifiers.available"] = subscriptionIdentifiersAvailable;
        }

        if (capabilities.SharedSubscriptionAvailable is { } sharedSubscriptionAvailable)
        {
            data["broker.shared.subscription.available"] = sharedSubscriptionAvailable;
        }

        if (capabilities.ServerKeepAlive is { } serverKeepAlive)
        {
            data["broker.server.keep_alive"] = serverKeepAlive;
        }

        if (capabilities.ResponseInformation is { } responseInformation)
        {
            data["broker.response.information"] = responseInformation;
        }

        if (capabilities.ServerReference is { } serverReference)
        {
            data["broker.server.reference"] = serverReference;
        }

        if (capabilities.AuthenticationMethod is { } authenticationMethod)
        {
            data["broker.authentication.method"] = authenticationMethod;
        }
    }

    private static string UnhealthyDescription(MqttClientDiagnosticsSnapshot snapshot)
    {
        if (snapshot.LastReason is { } reason)
        {
            return $"The client is {snapshot.State} ({reason}).";
        }

        return $"The client is {snapshot.State}.";
    }

    private readonly record struct PolicyResult(HealthStatus Status, string[] Reasons);
}
