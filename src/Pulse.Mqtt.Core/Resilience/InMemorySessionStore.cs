namespace Pulse.Mqtt.Resilience;

/// <summary>Session state held in memory for the lifetime of the process. The default store.</summary>
public sealed class InMemorySessionStore : ISessionStore
{
    private readonly object _gate = new();
    private IReadOnlyList<MqttTopicFilter> _subscriptions = [];

    /// <inheritdoc />
    public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);

        var copy = topicFilters.ToArray();
        lock (_gate)
        {
            _subscriptions = copy;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return ValueTask.FromResult(_subscriptions);
        }
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _subscriptions = [];
        }

        return ValueTask.CompletedTask;
    }
}
