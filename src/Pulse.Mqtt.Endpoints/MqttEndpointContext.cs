using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Everything one endpoint invocation can reach: the message, the typed route values, an optional
/// per-message service scope, and the dispatch cancellation token. This is also the surface the
/// upcoming source generator lowers Minimal-API-style handler signatures onto.
/// </summary>
public sealed class MqttEndpointContext
{
    private readonly IServiceProvider? _services;

    internal MqttEndpointContext(
        MqttPublishPacket message,
        MqttRouteValues values,
        IServiceProvider? services,
        CancellationToken cancellationToken)
    {
        Message = message;
        Route = new MqttEndpointRouteValues(values);
        _services = services;
        CancellationToken = cancellationToken;
    }

    /// <summary>The received PUBLISH packet.</summary>
    public MqttPublishPacket Message { get; }

    /// <summary>The topic the message arrived on.</summary>
    public string Topic => Message.Topic;

    /// <summary>The route parameters the endpoint's template captured, with typed accessors.</summary>
    public MqttEndpointRouteValues Route { get; }

    /// <summary>Whether <see cref="Services"/> is available for this invocation.</summary>
    public bool HasServices => _services is not null;

    /// <summary>
    /// The services for this message. When the endpoint was mapped through a host
    /// (<c>app.MapMqtt</c>) or with an explicit provider, each invocation gets its own scope, so
    /// scoped services behave exactly as they do in an ASP.NET Core request.
    /// </summary>
    /// <exception cref="InvalidOperationException">The endpoint was mapped without a service provider.</exception>
    public IServiceProvider Services =>
        _services ?? throw new InvalidOperationException(
            "This endpoint was mapped without services. Map it through a host (app.MapMqtt) or pass " +
            "an IServiceProvider to MapMqtt to resolve services per message.");

    /// <summary>The token that cancels when the client stops dispatching.</summary>
    public CancellationToken CancellationToken { get; }
}
