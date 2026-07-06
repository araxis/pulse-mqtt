using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
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
    private readonly MqttAcknowledgedRoutedMessage? _acknowledged;

    internal MqttEndpointContext(
        MqttPublishPacket message,
        MqttRouteValues values,
        IServiceProvider? services,
        CancellationToken cancellationToken,
        MqttAcknowledgedRoutedMessage? acknowledged = null)
    {
        Message = message;
        Route = new MqttEndpointRouteValues(values);
        _services = services;
        CancellationToken = cancellationToken;
        _acknowledged = acknowledged;
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

    /// <summary>How this endpoint invocation controls MQTT protocol acknowledgement.</summary>
    public MqttAcknowledgementMode Acknowledgement =>
        _acknowledged is null ? MqttAcknowledgementMode.Automatic : MqttAcknowledgementMode.Manual;

    /// <summary>Whether this message has already sent, or skipped, its protocol acknowledgement.</summary>
    public bool IsAcknowledged => _acknowledged?.IsAcknowledged ?? true;

    /// <summary>Whether this delivery can send a protocol-level negative acknowledgement.</summary>
    public bool CanReject => _acknowledged?.CanReject ?? false;

    /// <summary>Acknowledges successful processing of a manual-acknowledgement endpoint message.</summary>
    /// <exception cref="InvalidOperationException">The endpoint uses automatic acknowledgement.</exception>
    public ValueTask AcknowledgeAsync(CancellationToken cancellationToken = default) =>
        ManualDelivery().AcknowledgeAsync(cancellationToken);

    /// <summary>Rejects a manual-acknowledgement endpoint message with an MQTT 5 failure reason where possible.</summary>
    /// <exception cref="InvalidOperationException">The endpoint uses automatic acknowledgement.</exception>
    public ValueTask RejectAsync(
        MqttReasonCode reasonCode = MqttReasonCode.UnspecifiedError,
        string? reasonString = null,
        CancellationToken cancellationToken = default) =>
        ManualDelivery().RejectAsync(reasonCode, reasonString, cancellationToken);

    private MqttAcknowledgedRoutedMessage ManualDelivery() =>
        _acknowledged ?? throw new InvalidOperationException(
            "This endpoint uses automatic acknowledgement. Set MqttEndpointOptions.Acknowledgement to Manual before calling acknowledgement methods.");
}
