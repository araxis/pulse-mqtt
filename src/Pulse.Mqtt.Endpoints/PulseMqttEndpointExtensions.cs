using Microsoft.Extensions.DependencyInjection;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Maps Minimal-API-style endpoints directly on a <see cref="ResilientMqttClient"/>: one call
/// subscribes the template's filter and registers the local route. This is the core surface —
/// the <c>IHost</c> overloads are thin helpers over it.
/// </summary>
public static class PulseMqttEndpointExtensions
{
    /// <summary>
    /// Maps <paramref name="template"/> (for example <c>sensors/{deviceId:int}/temp</c>) to
    /// <paramref name="handler"/>: subscribes the matching filter and dispatches matching
    /// messages with an <see cref="MqttEndpointContext"/>. When <paramref name="services"/> is
    /// given, every invocation runs in its own service scope.
    /// </summary>
    public static MqttEndpoint MapMqtt(
        this ResilientMqttClient client,
        string template,
        Func<MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null,
        IServiceProvider? services = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(handler);

        var route = MqttRouteTemplate.Parse(template);
        var endpointOptions = options ?? Defaults;
        var invoker = new ScopedInvoker(services);
        var registration = endpointOptions.Acknowledgement == MqttAcknowledgementMode.Manual
            ? client.RegisterManualAcknowledgementRoute(
                route,
                (message, token) => invoker.InvokeAsync(handler, message, token),
                endpointOptions.Route)
            : client.RegisterRoute(
                route,
                (message, values, token) => invoker.InvokeAsync(handler, message, values, token),
                endpointOptions.Route);

        return Complete(client, route, registration, endpointOptions);
    }

    /// <summary>
    /// Maps <paramref name="template"/> to a typed handler: the payload is deserialized with the
    /// client's configured serializer before <paramref name="handler"/> runs, exactly like
    /// <c>RegisterRoute&lt;T&gt;</c>. When <paramref name="services"/> is given, every invocation
    /// runs in its own service scope.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public static MqttEndpoint MapMqtt<TPayload>(
        this ResilientMqttClient client,
        string template,
        Func<TPayload, MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null,
        IServiceProvider? services = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(handler);

        var route = MqttRouteTemplate.Parse(template);
        var endpointOptions = options ?? Defaults;
        var invoker = new ScopedInvoker(services);
        var serializer = endpointOptions.Acknowledgement == MqttAcknowledgementMode.Manual
            ? client.SerializerOrThrow()
            : null;
        var registration = endpointOptions.Acknowledgement == MqttAcknowledgementMode.Manual
            ? client.RegisterManualAcknowledgementRoute(
                route,
                (routed, token) =>
                {
                    var payload = serializer!.Deserialize<TPayload>(routed.Message.Payload);
                    return invoker.InvokeAsync(context => handler(payload, context), routed, token);
                },
                endpointOptions.Route)
            : client.RegisterRoute<TPayload>(
                route,
                (payload, routed, token) => invoker.InvokeAsync(
                    context => handler(payload, context), routed.Message, routed.Values, token),
                endpointOptions.Route);

        return Complete(client, route, registration, endpointOptions);
    }

    /// <summary>
    /// Maps a request/reply endpoint: each matching request is deserialized to
    /// <typeparamref name="TRequest"/>, handled, and the returned <typeparamref name="TResponse"/>
    /// serialized and published to the request's response topic with its correlation data echoed —
    /// the Minimal-API model where the handler's return value is the response. Requests without a
    /// response topic are ignored. When <paramref name="services"/> is given, every invocation runs
    /// in its own service scope. A handler exception sends no reply; the requester's timeout governs.
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public static MqttEndpoint MapMqttRequest<TRequest, TResponse>(
        this ResilientMqttClient client,
        string template,
        Func<TRequest, MqttEndpointContext, ValueTask<TResponse>> handler,
        MqttEndpointOptions? options = null,
        IServiceProvider? services = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(handler);

        var route = MqttRouteTemplate.Parse(template);
        var endpointOptions = options ?? Defaults;
        ThrowIfManualRequestAcknowledgement(endpointOptions);
        var invoker = new ScopedInvoker(services);
        var registration = client.RegisterRequestHandler<TRequest, TResponse>(
            route,
            (request, routed, token) => invoker.InvokeAsync(
                context => handler(request, context), routed.Message, routed.Values, token),
            endpointOptions.Route);

        return Complete(client, route, registration, endpointOptions);
    }

    private static readonly MqttEndpointOptions Defaults = new();

    private static MqttEndpoint Complete(
        ResilientMqttClient client,
        MqttRouteTemplate route,
        IDisposable registration,
        MqttEndpointOptions endpointOptions)
    {
        var filter = route.ToTopicFilter(
            endpointOptions.QualityOfService,
            endpointOptions.NoLocal,
            endpointOptions.RetainAsPublished,
            endpointOptions.RetainHandling);

        // Subscriptions are reference-counted per client: distinct templates can share one
        // filter, and only the last endpoint's disposal may unsubscribe it. An empty grant
        // result means the client was offline and queued the subscription for the next
        // connection — the endpoint is live either way; a failure reason code is a denial.
        var subscriptions = EndpointSubscriptions.For(client);
        return new MqttEndpoint(
            client, route, registration, subscriptions,
            subscriptions.SubscribeAsync(client, route, filter));
    }

    private static void ThrowIfManualRequestAcknowledgement(MqttEndpointOptions options)
    {
        if (options.Acknowledgement == MqttAcknowledgementMode.Manual)
        {
            throw new ArgumentException(
                "Manual acknowledgement is not supported for request/reply endpoints.",
                nameof(options));
        }
    }

    /// <summary>Runs each invocation in its own service scope when a provider is present.</summary>
    private sealed class ScopedInvoker(IServiceProvider? services)
    {
        private readonly IServiceScopeFactory? _scopes = services?.GetService<IServiceScopeFactory>();

        public async ValueTask InvokeAsync(
            Func<MqttEndpointContext, ValueTask> handler,
            MqttPublishPacket message,
            MqttRouteValues values,
            CancellationToken cancellationToken)
        {
            if (_scopes is null)
            {
                await handler(new MqttEndpointContext(message, values, services, cancellationToken)).ConfigureAwait(false);
                return;
            }

            var scope = _scopes.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                await handler(new MqttEndpointContext(message, values, scope.ServiceProvider, cancellationToken)).ConfigureAwait(false);
            }
        }

        public async ValueTask InvokeAsync(
            Func<MqttEndpointContext, ValueTask> handler,
            MqttAcknowledgedRoutedMessage routed,
            CancellationToken cancellationToken)
        {
            if (_scopes is null)
            {
                await handler(new MqttEndpointContext(
                        routed.Message,
                        routed.Values,
                        services,
                        cancellationToken,
                        routed))
                    .ConfigureAwait(false);
                return;
            }

            var scope = _scopes.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                await handler(new MqttEndpointContext(
                        routed.Message,
                        routed.Values,
                        scope.ServiceProvider,
                        cancellationToken,
                        routed))
                    .ConfigureAwait(false);
            }
        }

        public async ValueTask<TResponse> InvokeAsync<TResponse>(
            Func<MqttEndpointContext, ValueTask<TResponse>> handler,
            MqttPublishPacket message,
            MqttRouteValues values,
            CancellationToken cancellationToken)
        {
            if (_scopes is null)
            {
                return await handler(new MqttEndpointContext(message, values, services, cancellationToken)).ConfigureAwait(false);
            }

            var scope = _scopes.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                return await handler(new MqttEndpointContext(message, values, scope.ServiceProvider, cancellationToken)).ConfigureAwait(false);
            }
        }
    }
}
