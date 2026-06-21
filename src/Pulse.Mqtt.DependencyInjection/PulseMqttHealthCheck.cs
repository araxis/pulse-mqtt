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

        if (snapshot.BrokerCapabilities is { } capabilities)
        {
            AddBrokerCapabilities(data, capabilities);
        }

        return data;
    }

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
}
