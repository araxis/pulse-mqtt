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

    /// <summary>
    /// Adds <paramref name="topicFilters"/> to the stored set, replacing entries with the same
    /// topic. The default reads and rewrites the whole set; implementations that can update in
    /// place should override to avoid the full copy per call.
    /// </summary>
    async ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);

        var current = await LoadSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
        var merged = new Dictionary<string, MqttTopicFilter>(current.Count + topicFilters.Count);
        foreach (var filter in current)
        {
            merged[filter.Topic] = filter;
        }

        foreach (var filter in topicFilters)
        {
            merged[filter.Topic] = filter;
        }

        await SaveSubscriptionsAsync([.. merged.Values], cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes the subscriptions for <paramref name="topics"/> from the stored set. The default
    /// reads and rewrites the whole set; implementations that can update in place should override.
    /// </summary>
    async ValueTask RemoveSubscriptionsAsync(IReadOnlyList<string> topics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topics);

        var current = await LoadSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
        var removed = new HashSet<string>(topics);
        var remaining = new List<MqttTopicFilter>(current.Count);
        foreach (var filter in current)
        {
            if (!removed.Contains(filter.Topic))
            {
                remaining.Add(filter);
            }
        }

        await SaveSubscriptionsAsync(remaining, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Persists the session's unfinished QoS state so a resume can redeliver. The default keeps
    /// nothing — a store that does not override these gets today's behavior (no redelivery
    /// across restarts); the in-memory default and durable stores implement them.
    /// </summary>
    ValueTask SaveInFlightAsync(MqttInFlightState state, CancellationToken cancellationToken) => default;

    /// <summary>Returns the stored in-flight state, or <see langword="null"/> when nothing was saved.</summary>
    ValueTask<MqttInFlightState?> LoadInFlightAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult<MqttInFlightState?>(null);
}
