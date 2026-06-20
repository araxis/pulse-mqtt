namespace Pulse.Mqtt.Client;

/// <summary>
/// A route created by the endpoint-style <c>OnAsync</c> fluent methods. Async disposal
/// unregisters the local route and unsubscribes the broker filter.
/// </summary>
public sealed class MqttSubscribedRoute : IAsyncDisposable
{
    private readonly ResilientMqttClient _client;
    private readonly IDisposable _route;
    private int _disposed;

    internal MqttSubscribedRoute(
        ResilientMqttClient client,
        IDisposable route,
        MqttTopicFilter topicFilter)
    {
        _client = client;
        _route = route;
        TopicFilter = topicFilter;
    }

    /// <summary>The broker filter subscribed for this route.</summary>
    public MqttTopicFilter TopicFilter { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _route.Dispose();
        await ResilientMqttClientFluentExtensions.TryUnsubscribeAsync(_client, TopicFilter.Topic)
            .ConfigureAwait(false);
    }
}
