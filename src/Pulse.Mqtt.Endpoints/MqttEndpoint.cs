using Pulse.Mqtt.Client;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// One mapped endpoint: the broker subscription plus the local route created by <c>MapMqtt</c>.
/// Await <see cref="Subscribed"/> where startup must fail fast on a denied subscription; dispose
/// to unregister the route and unsubscribe the filter.
/// </summary>
public sealed class MqttEndpoint : IAsyncDisposable
{
    private readonly ResilientMqttClient _client;
    private readonly IDisposable _route;

    internal MqttEndpoint(ResilientMqttClient client, MqttRouteTemplate template, IDisposable route, Task subscribed)
    {
        _client = client;
        _route = route;
        Template = template;
        Subscribed = subscribed;
    }

    /// <summary>The endpoint's route template.</summary>
    public MqttRouteTemplate Template { get; }

    /// <summary>
    /// Completes when the broker granted the subscription (or when it was queued for the next
    /// connection while offline); faults when the broker denied it. Awaiting is optional — an
    /// offline map still subscribes on reconnect — but a fail-fast startup should await it.
    /// </summary>
    public Task Subscribed { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _route.Dispose();
        try
        {
            await _client.UnsubscribeAsync([Template.TopicFilter], CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
        {
            // Teardown path: the client may already be disposed or offline; the durable
            // subscription set still forgets the filter, which is what matters here.
        }
    }
}
