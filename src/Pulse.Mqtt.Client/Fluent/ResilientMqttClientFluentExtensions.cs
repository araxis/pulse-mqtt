using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent entry points on <see cref="ResilientMqttClient"/>. Each returns a small builder that
/// composes one operation and runs it through the client's regular APIs — same semantics, same
/// guarantees, just chainable.
/// </summary>
public static class ResilientMqttClientFluentExtensions
{
    /// <summary>
    /// Subscribes the broker filter derived from <paramref name="template"/> with QoS 0.
    /// This subscribes broker delivery only; register local routes separately.
    /// </summary>
    public static Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        this ResilientMqttClient client,
        MqttRouteTemplate template) =>
        SubscribeAsync(client, template, MqttQualityOfService.AtMostOnce, CancellationToken.None);

    /// <summary>
    /// Subscribes the broker filter derived from <paramref name="template"/> with QoS 0.
    /// This subscribes broker delivery only; register local routes separately.
    /// </summary>
    public static Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        CancellationToken cancellationToken) =>
        SubscribeAsync(client, template, MqttQualityOfService.AtMostOnce, cancellationToken);

    /// <summary>
    /// Subscribes the broker filter derived from <paramref name="template"/> with
    /// <paramref name="maximumQualityOfService"/>.
    /// </summary>
    public static Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttQualityOfService maximumQualityOfService) =>
        SubscribeAsync(client, template, maximumQualityOfService, CancellationToken.None);

    /// <summary>
    /// Subscribes the broker filter derived from <paramref name="template"/> with
    /// <paramref name="maximumQualityOfService"/>.
    /// </summary>
    public static Task<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttQualityOfService maximumQualityOfService,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(template);
        return client.SubscribeAsync([template.ToTopicFilter(maximumQualityOfService)], cancellationToken);
    }

    /// <summary>Starts composing a publish to <paramref name="topic"/>.</summary>
    public static MqttPublishBuilder Publish(this ResilientMqttClient client, string topic)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return new MqttPublishBuilder(client, topic);
    }

    /// <summary>Starts composing a route for <paramref name="template"/> (for example <c>sensors/{deviceId}/temp</c>).</summary>
    public static MqttRouteBuilder Route(this ResilientMqttClient client, string template)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(template);
        return new MqttRouteBuilder(client, template);
    }

    /// <summary>
    /// Subscribes the route's broker filter and registers a raw local handler in one endpoint-style
    /// call. Dispose the returned handle to remove the local route and unsubscribe the broker filter.
    /// </summary>
    public static Task<MqttSubscribedRoute> OnAsync(
        this ResilientMqttClient client,
        string template,
        MqttRouteHandler handler) =>
        OnAsync(client, template, MqttQualityOfService.AtMostOnce, handler, CancellationToken.None);

    /// <summary>
    /// Subscribes the route's broker filter and registers a raw local handler in one endpoint-style
    /// call. Dispose the returned handle to remove the local route and unsubscribe the broker filter.
    /// </summary>
    public static Task<MqttSubscribedRoute> OnAsync(
        this ResilientMqttClient client,
        string template,
        MqttRouteHandler handler,
        CancellationToken cancellationToken) =>
        OnAsync(client, template, MqttQualityOfService.AtMostOnce, handler, cancellationToken);

    /// <summary>
    /// Subscribes the route's broker filter with <paramref name="maximumQualityOfService"/> and
    /// registers a raw local handler in one endpoint-style call.
    /// </summary>
    public static Task<MqttSubscribedRoute> OnAsync(
        this ResilientMqttClient client,
        string template,
        MqttQualityOfService maximumQualityOfService,
        MqttRouteHandler handler) =>
        OnAsync(client, template, maximumQualityOfService, handler, CancellationToken.None);

    /// <summary>
    /// Subscribes the route's broker filter with <paramref name="maximumQualityOfService"/> and
    /// registers a raw local handler in one endpoint-style call.
    /// </summary>
    public static Task<MqttSubscribedRoute> OnAsync(
        this ResilientMqttClient client,
        string template,
        MqttQualityOfService maximumQualityOfService,
        MqttRouteHandler handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(template);
        ArgumentNullException.ThrowIfNull(handler);
        var routeTemplate = MqttRouteTemplate.Parse(template);
        return OnCoreAsync(
            client,
            routeTemplate,
            routeTemplate.ToTopicFilter(maximumQualityOfService),
            () => client.RegisterRoute(routeTemplate, handler),
            cancellationToken);
    }

    /// <summary>
    /// Subscribes the route's broker filter and registers a typed local handler in one
    /// endpoint-style call. Dispose the returned handle to remove the local route and unsubscribe
    /// the broker filter.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public static Task<MqttSubscribedRoute> OnAsync<T>(
        this ResilientMqttClient client,
        string template,
        MqttTypedRouteHandler<T> handler) =>
        OnAsync(client, template, MqttQualityOfService.AtMostOnce, handler, CancellationToken.None);

    /// <summary>
    /// Subscribes the route's broker filter and registers a typed local handler in one
    /// endpoint-style call. Dispose the returned handle to remove the local route and unsubscribe
    /// the broker filter.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public static Task<MqttSubscribedRoute> OnAsync<T>(
        this ResilientMqttClient client,
        string template,
        MqttTypedRouteHandler<T> handler,
        CancellationToken cancellationToken) =>
        OnAsync(client, template, MqttQualityOfService.AtMostOnce, handler, cancellationToken);

    /// <summary>
    /// Subscribes the route's broker filter with <paramref name="maximumQualityOfService"/> and
    /// registers a typed local handler in one endpoint-style call.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public static Task<MqttSubscribedRoute> OnAsync<T>(
        this ResilientMqttClient client,
        string template,
        MqttQualityOfService maximumQualityOfService,
        MqttTypedRouteHandler<T> handler) =>
        OnAsync(client, template, maximumQualityOfService, handler, CancellationToken.None);

    /// <summary>
    /// Subscribes the route's broker filter with <paramref name="maximumQualityOfService"/> and
    /// registers a typed local handler in one endpoint-style call.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public static Task<MqttSubscribedRoute> OnAsync<T>(
        this ResilientMqttClient client,
        string template,
        MqttQualityOfService maximumQualityOfService,
        MqttTypedRouteHandler<T> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(template);
        ArgumentNullException.ThrowIfNull(handler);
        var routeTemplate = MqttRouteTemplate.Parse(template);
        return OnCoreAsync(
            client,
            routeTemplate,
            routeTemplate.ToTopicFilter(maximumQualityOfService),
            () => client.RegisterRoute(routeTemplate, handler),
            cancellationToken);
    }

    /// <summary>Starts composing a request to <paramref name="topic"/>.</summary>
    public static MqttRequestBuilder Request(this ResilientMqttClient client, string topic)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return new MqttRequestBuilder(client, topic);
    }

    private static async Task<MqttSubscribedRoute> OnCoreAsync(
        ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttTopicFilter topicFilter,
        Func<IDisposable> registerRoute,
        CancellationToken cancellationToken)
    {
        IDisposable? route = null;
        try
        {
            route = registerRoute();
            var granted = await client.SubscribeAsync([topicFilter], cancellationToken)
                .ConfigureAwait(false);
            ThrowIfSubscribeFailed(template, granted);
            return new MqttSubscribedRoute(client, route, topicFilter);
        }
        catch
        {
            route?.Dispose();
            if (route is not null)
            {
                await TryUnsubscribeAsync(client, topicFilter.Topic).ConfigureAwait(false);
            }

            throw;
        }
    }

    private static void ThrowIfSubscribeFailed(
        MqttRouteTemplate template,
        IReadOnlyList<MqttReasonCode> granted)
    {
        var failed = granted.FirstOrDefault(reason => (byte)reason >= 128);
        if ((byte)failed >= 128)
        {
            throw new MqttException(
                $"MQTT subscribe for route '{template.Template}' failed: {failed}.");
        }
    }

    internal static async ValueTask TryUnsubscribeAsync(
        ResilientMqttClient client,
        string topicFilter)
    {
        try
        {
            await client.UnsubscribeAsync([topicFilter], CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }
        catch (OperationCanceledException)
        {
        }
    }
}
