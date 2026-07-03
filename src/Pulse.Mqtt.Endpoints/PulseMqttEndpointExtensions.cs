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
        var invoker = new ScopedInvoker(services);
        var registration = client.RegisterRoute(
            route,
            (message, values, token) => invoker.InvokeAsync(handler, message, values, token),
            (options ?? Defaults).Route);

        return Complete(client, route, registration, options);
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
        var invoker = new ScopedInvoker(services);
        var registration = client.RegisterRoute<TPayload>(
            route,
            (payload, routed, token) => invoker.InvokeAsync(
                context => handler(payload, context), routed.Message, routed.Values, token),
            (options ?? Defaults).Route);

        return Complete(client, route, registration, options);
    }

    private static readonly MqttEndpointOptions Defaults = new();

    private static MqttEndpoint Complete(
        ResilientMqttClient client,
        MqttRouteTemplate route,
        IDisposable registration,
        MqttEndpointOptions? options)
    {
        var endpointOptions = options ?? Defaults;
        var filter = route.ToTopicFilter(
            endpointOptions.QualityOfService,
            endpointOptions.NoLocal,
            endpointOptions.RetainAsPublished,
            endpointOptions.RetainHandling);

        return new MqttEndpoint(client, route, registration, SubscribeAsync(client, route, filter));
    }

    private static async Task SubscribeAsync(
        ResilientMqttClient client,
        MqttRouteTemplate route,
        MqttTopicFilter filter)
    {
        // An empty result means the client was offline and queued the subscription for the next
        // connection — the endpoint is live either way. A failure reason code is a denial.
        var granted = await client.SubscribeAsync([filter], CancellationToken.None).ConfigureAwait(false);
        if (granted.Count > 0 && (byte)granted[0] >= 0x80)
        {
            throw new MqttException($"The broker denied the subscription for '{route.Template}': {granted[0]}.");
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
    }
}
