namespace Pulse.Mqtt.Resilience;

/// <summary>
/// Holds the client's durable session state so a reconnect can restore it. This is a swap point:
/// the in-memory default keeps state for the process lifetime; durable implementations persist it
/// across restarts.
/// </summary>
public interface ISessionStore
{
    /// <summary>Replaces the stored subscription set with <paramref name="topicFilters"/>.</summary>
    ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken);

    /// <summary>Returns the stored subscription set; empty when nothing was saved.</summary>
    ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken);

    /// <summary>Removes all stored session state.</summary>
    ValueTask ClearAsync(CancellationToken cancellationToken);
}
