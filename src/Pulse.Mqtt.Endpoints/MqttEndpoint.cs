using Pulse.Mqtt.Client;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// One mapped endpoint: the broker subscription plus the local route created by <c>MapMqtt</c>.
/// Await <see cref="Subscribed"/> where startup must fail fast on a denied subscription; dispose
/// to unregister the route and release the filter (unsubscribing when no other endpoint uses it).
/// </summary>
public sealed class MqttEndpoint : IAsyncDisposable
{
    private readonly ResilientMqttClient _client;
    private readonly IDisposable _route;
    private readonly EndpointSubscriptions _subscriptions;

    internal MqttEndpoint(
        ResilientMqttClient client,
        MqttRouteTemplate template,
        IDisposable route,
        EndpointSubscriptions subscriptions,
        Task subscribed)
    {
        _client = client;
        _route = route;
        _subscriptions = subscriptions;
        Template = template;
        Subscribed = subscribed;

        // Awaiting Subscribed is documented as optional, so a denial must not surface as an
        // UnobservedTaskException at finalization; observing here leaves awaiters unaffected.
        _ = subscribed.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
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

        // Only release a subscription reference this endpoint actually took. A denied subscription
        // faults Subscribed and takes no reference (SubscribeAsync throws before the increment), so
        // releasing here would decrement — and possibly unsubscribe — a filter another endpoint
        // still holds. Awaiting Subscribed first also preserves gate ordering: the subscribe is
        // enqueued on the shared gate before this release.
        try
        {
            await Subscribed.ConfigureAwait(false);
        }
        catch
        {
            // Denied (or otherwise never subscribed): no reference to release.
            return;
        }

        try
        {
            await _subscriptions.ReleaseAsync(_client, Template.TopicFilter).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is MqttException or InvalidOperationException or ObjectDisposedException)
        {
            // Teardown path: the client may already be disposed or offline; the durable
            // subscription set still forgets the filter, which is what matters here.
        }
    }
}
