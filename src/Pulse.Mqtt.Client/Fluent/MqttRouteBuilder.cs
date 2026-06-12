using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent registration of one route. Created by
/// <see cref="ResilientMqttClientFluentExtensions.Route"/>; finished with a handler or stream
/// terminal, which registers the route and subscribes its filter.
/// </summary>
/// <example>
/// <code>
/// using var route = await client.Route("sensors/{deviceId}/temp")
///     .WithConcurrency(4)
///     .WithQueue(capacity: 128, overflow: RouteOverflow.DropOldest)
///     .HandleAsync&lt;TelemetryReading&gt;((reading, message, ct) =>
///         Handle(reading, message.Values["deviceId"]));
/// </code>
/// </example>
public sealed class MqttRouteBuilder
{
    private readonly ResilientMqttClient _client;
    private readonly string _template;
    private MqttRouteOptions _options = new();

    internal MqttRouteBuilder(ResilientMqttClient client, string template)
    {
        _client = client;
        _template = template;
    }

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

    /// <summary>The QoS requested when the route's filter is subscribed.</summary>
    public MqttRouteBuilder WithSubscriptionQualityOfService(MqttQualityOfService qualityOfService)
    {
        _options = _options with { SubscriptionQualityOfService = qualityOfService };
        return this;
    }

    /// <summary>Registers a raw handler and subscribes the filter. Dispose the result to remove the route.</summary>
    public Task<IDisposable> HandleAsync(MqttRouteHandler handler, CancellationToken cancellationToken = default) =>
        _client.OnAsync(_template, handler, _options, cancellationToken);

    /// <summary>Registers a typed handler and subscribes the filter. Dispose the result to remove the route.</summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public Task<IDisposable> HandleAsync<T>(MqttTypedRouteHandler<T> handler, CancellationToken cancellationToken = default) =>
        _client.OnAsync(_template, handler, _options, cancellationToken);

    /// <summary>Opens an <c>await foreach</c>-able stream for the route and subscribes the filter.</summary>
    public Task<MqttRouteStream> StreamAsync(CancellationToken cancellationToken = default) =>
        _client.OpenStreamAsync(_template, _options, cancellationToken);
}
