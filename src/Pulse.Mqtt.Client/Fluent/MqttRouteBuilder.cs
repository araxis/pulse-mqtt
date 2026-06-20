using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent registration of one route. Created by
/// <see cref="ResilientMqttClientFluentExtensions.Route"/>; finished with a handler or stream
/// terminal, which registers a local route. Subscribe the matching broker filter separately.
/// </summary>
/// <example>
/// <code>
/// var template = client.Route("sensors/{deviceId}/temp");
/// await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], token);
/// using var route = template
///     .WithConcurrency(4)
///     .WithQueue(capacity: 128, overflow: RouteOverflow.DropOldest)
///     .Handle&lt;TelemetryReading&gt;((reading, message, ct) =>
///         Handle(reading, message.Values["deviceId"]));
/// </code>
/// </example>
public sealed class MqttRouteBuilder
{
    private readonly ResilientMqttClient _client;
    private readonly MqttRouteTemplate _template;
    private MqttRouteOptions _options = new();

    internal MqttRouteBuilder(ResilientMqttClient client, string template)
    {
        _client = client;
        _template = MqttRouteTemplate.Parse(template);
    }

    /// <summary>Creates the broker subscription filter that delivers messages matching this route.</summary>
    public MqttTopicFilter ToTopicFilter(
        MqttQualityOfService maximumQualityOfService = MqttQualityOfService.AtMostOnce,
        bool noLocal = false,
        bool retainAsPublished = false,
        MqttRetainHandling retainHandling = MqttRetainHandling.SendAtSubscribe) =>
        _template.ToTopicFilter(maximumQualityOfService, noLocal, retainAsPublished, retainHandling);

    /// <summary>Bounds the route's queue and picks its overflow behavior.</summary>
    public MqttRouteBuilder WithQueue(int capacity, RouteOverflow overflow = RouteOverflow.Wait)
    {
        _options = _options with { Capacity = capacity, Overflow = overflow };
        return this;
    }

    /// <summary>Allows up to <paramref name="maxConcurrency"/> handler invocations at once. 1 preserves order.</summary>
    public MqttRouteBuilder WithConcurrency(int maxConcurrency)
    {
        _options = _options with { MaxConcurrency = maxConcurrency };
        return this;
    }

    /// <summary>Registers a raw local handler. Dispose the result to remove the route.</summary>
    public IDisposable Handle(MqttRouteHandler handler) =>
        _client.RegisterRoute(_template, handler, _options);

    /// <summary>Registers a typed local handler. Dispose the result to remove the route.</summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public IDisposable Handle<T>(MqttTypedRouteHandler<T> handler) =>
        _client.RegisterRoute(_template, handler, _options);

    /// <summary>Opens an <c>await foreach</c>-able stream for the local route.</summary>
    public MqttRouteStream Stream() =>
        _client.OpenRouteStream(_template, _options);
}
