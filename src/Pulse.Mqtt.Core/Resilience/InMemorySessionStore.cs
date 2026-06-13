namespace Pulse.Mqtt.Resilience;

/// <summary>Session state held in memory for the lifetime of the process. The default store.</summary>
public sealed class InMemorySessionStore : ISessionStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, MqttTopicFilter> _subscriptions = [];
    private IReadOnlyList<MqttTopicFilter>? _snapshot;
    private MqttInFlightState? _inFlight;

    /// <inheritdoc />
    public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);

        lock (_gate)
        {
            _subscriptions.Clear();
            foreach (var filter in topicFilters)
            {
                _subscriptions[filter.Topic] = filter;
            }

            _snapshot = null;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return ValueTask.FromResult(_snapshot ??= [.. _subscriptions.Values]);
        }
    }

    /// <inheritdoc />
    public ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);

        lock (_gate)
        {
            foreach (var filter in topicFilters)
            {
                _subscriptions[filter.Topic] = filter;
            }

            _snapshot = null;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RemoveSubscriptionsAsync(IReadOnlyList<string> topics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topics);

        lock (_gate)
        {
            foreach (var topic in topics)
            {
                _subscriptions.Remove(topic);
            }

            _snapshot = null;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask SaveInFlightAsync(MqttInFlightState state, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        lock (_gate)
        {
            _inFlight = state;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<MqttInFlightState?> LoadInFlightAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return ValueTask.FromResult(_inFlight);
        }
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _subscriptions.Clear();
            _snapshot = null;
            _inFlight = null;
        }

        return ValueTask.CompletedTask;
    }
}
