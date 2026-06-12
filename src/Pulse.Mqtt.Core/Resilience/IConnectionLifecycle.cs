using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// Hooks that run as a resilient connection comes up and goes down. The default re-subscribes
/// from the <see cref="ISessionStore"/>; replace it to add application-level priming. This is a
/// swap point.
/// </summary>
public interface IConnectionLifecycle
{
    /// <summary>
    /// Runs after a successful CONNACK and before queued publishes flush, so re-subscription is in
    /// place before any traffic. <paramref name="context"/> carries the negotiated session.
    /// </summary>
    ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken cancellationToken);

    /// <summary>Runs when the connection is lost, with the reason when one is known.</summary>
    ValueTask OnConnectionDownAsync(MqttReasonCode? reason, CancellationToken cancellationToken);
}

/// <summary>What a lifecycle hook sees when a connection comes up.</summary>
public interface IConnectionUpContext
{
    /// <summary>The broker's CONNACK, including <see cref="MqttConnAckPacket.SessionPresent"/> and negotiated limits.</summary>
    MqttConnAckPacket ConnAck { get; }

    /// <summary>The connection attempt counter (0 on the first connect).</summary>
    int Attempt { get; }

    /// <summary>Re-establishes subscriptions on the new connection.</summary>
    ISubscriptionRegistrar Subscriptions { get; }
}

/// <summary>Subscribes topic filters on the live connection during connection-up handling.</summary>
public interface ISubscriptionRegistrar
{
    /// <summary>Subscribes <paramref name="topicFilters"/> and returns the broker's per-filter results.</summary>
    ValueTask<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        IReadOnlyList<MqttTopicFilter> topicFilters,
        CancellationToken cancellationToken);
}
