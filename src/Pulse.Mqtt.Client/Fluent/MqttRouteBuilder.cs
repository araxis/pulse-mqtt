using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent registration of one route. Created by
/// <see cref="ResilientMqttClientFluentExtensions.Route"/>; finished with a local handler/stream
/// terminal or an asynchronous terminal that also subscribes the matching broker filter.
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
    private MqttTopicFilter _topicFilter;
    private MqttRouteOptions _options = new();

    internal MqttRouteBuilder(ResilientMqttClient client, string template)
    {
        _client = client;
        _template = MqttRouteTemplate.Parse(template);
        _topicFilter = _template.ToTopicFilter();
    }

    /// <summary>Creates the broker subscription filter currently configured by this builder.</summary>
    public MqttTopicFilter ToTopicFilter() => _topicFilter;

    /// <summary>Creates the broker subscription filter that delivers messages matching this route.</summary>
    public MqttTopicFilter ToTopicFilter(
        MqttQualityOfService maximumQualityOfService = MqttQualityOfService.AtMostOnce,
        bool noLocal = false,
        bool retainAsPublished = false,
        MqttRetainHandling retainHandling = MqttRetainHandling.SendAtSubscribe) =>
        _template.ToTopicFilter(maximumQualityOfService, noLocal, retainAsPublished, retainHandling);

    /// <summary>Requests QoS 0 for the broker subscription created by asynchronous terminals.</summary>
    public MqttRouteBuilder AtMostOnce() => WithQualityOfService(MqttQualityOfService.AtMostOnce);

    /// <summary>Requests QoS 1 for the broker subscription created by asynchronous terminals.</summary>
    public MqttRouteBuilder AtLeastOnce() => WithQualityOfService(MqttQualityOfService.AtLeastOnce);

    /// <summary>Requests QoS 2 for the broker subscription created by asynchronous terminals.</summary>
    public MqttRouteBuilder ExactlyOnce() => WithQualityOfService(MqttQualityOfService.ExactlyOnce);

    /// <summary>Sets the maximum QoS for the broker subscription created by asynchronous terminals.</summary>
    public MqttRouteBuilder WithQualityOfService(MqttQualityOfService maximumQualityOfService)
    {
        _topicFilter = _topicFilter with { MaximumQualityOfService = maximumQualityOfService };
        return this;
    }

    /// <summary>Configures whether the broker should suppress this client's own publications.</summary>
    public MqttRouteBuilder WithNoLocal(bool noLocal = true)
    {
        _topicFilter = _topicFilter with { NoLocal = noLocal };
        return this;
    }

    /// <summary>Configures whether retained deliveries keep their retain flag.</summary>
    public MqttRouteBuilder WithRetainAsPublished(bool retainAsPublished = true)
    {
        _topicFilter = _topicFilter with { RetainAsPublished = retainAsPublished };
        return this;
    }

    /// <summary>Configures when retained messages are replayed for the broker subscription.</summary>
    public MqttRouteBuilder WithRetainHandling(MqttRetainHandling retainHandling)
    {
        _topicFilter = _topicFilter with { RetainHandling = retainHandling };
        return this;
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

    /// <summary>Subscribes the broker filter and registers a raw local handler.</summary>
    public Task<MqttSubscribedRoute> HandleAsync(MqttRouteHandler handler) =>
        HandleAsync(handler, CancellationToken.None);

    /// <summary>Subscribes the broker filter and registers a raw local handler.</summary>
    public Task<MqttSubscribedRoute> HandleAsync(
        MqttRouteHandler handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return ResilientMqttClientFluentExtensions.SubscribeRouteAsync(
            _client,
            _template,
            _topicFilter,
            () => _client.RegisterRoute(_template, handler, _options),
            cancellationToken);
    }

    /// <summary>Subscribes the broker filter and registers a typed local handler.</summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public Task<MqttSubscribedRoute> HandleAsync<T>(MqttTypedRouteHandler<T> handler) =>
        HandleAsync(handler, CancellationToken.None);

    /// <summary>Subscribes the broker filter and registers a typed local handler.</summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public Task<MqttSubscribedRoute> HandleAsync<T>(
        MqttTypedRouteHandler<T> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return ResilientMqttClientFluentExtensions.SubscribeRouteAsync(
            _client,
            _template,
            _topicFilter,
            () => _client.RegisterRoute(_template, handler, _options),
            cancellationToken);
    }

    /// <summary>Subscribes the broker filter and opens a stream for the local route.</summary>
    public Task<MqttSubscribedRouteStream> StreamAsync() =>
        StreamAsync(CancellationToken.None);

    /// <summary>Subscribes the broker filter and opens a stream for the local route.</summary>
    public Task<MqttSubscribedRouteStream> StreamAsync(CancellationToken cancellationToken) =>
        ResilientMqttClientFluentExtensions.SubscribeRouteStreamAsync(
            _client,
            _template,
            _topicFilter,
            () => _client.OpenRouteStream(_template, _options),
            cancellationToken);

    /// <summary>Switches this route to explicit protocol acknowledgement for subsequent terminals.</summary>
    public MqttManualAcknowledgementRouteBuilder ManualAcknowledgement() =>
        new(_client, _template, _topicFilter, _options);
}

/// <summary>Fluent terminals for a route that explicitly acknowledges or rejects deliveries.</summary>
public sealed class MqttManualAcknowledgementRouteBuilder
{
    private readonly ResilientMqttClient _client;
    private readonly MqttRouteTemplate _template;
    private MqttTopicFilter _topicFilter;
    private MqttRouteOptions _options;

    internal MqttManualAcknowledgementRouteBuilder(
        ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttTopicFilter topicFilter,
        MqttRouteOptions options)
    {
        _client = client;
        _template = template;
        _topicFilter = topicFilter;
        _options = options;
    }

    /// <summary>Creates the broker subscription filter currently configured by this builder.</summary>
    public MqttTopicFilter ToTopicFilter() => _topicFilter;

    /// <summary>Requests QoS 0 for the broker subscription created by asynchronous terminals.</summary>
    public MqttManualAcknowledgementRouteBuilder AtMostOnce() => WithQualityOfService(MqttQualityOfService.AtMostOnce);

    /// <summary>Requests QoS 1 for the broker subscription created by asynchronous terminals.</summary>
    public MqttManualAcknowledgementRouteBuilder AtLeastOnce() => WithQualityOfService(MqttQualityOfService.AtLeastOnce);

    /// <summary>Requests QoS 2 for the broker subscription created by asynchronous terminals.</summary>
    public MqttManualAcknowledgementRouteBuilder ExactlyOnce() => WithQualityOfService(MqttQualityOfService.ExactlyOnce);

    /// <summary>Sets the maximum QoS for the broker subscription created by asynchronous terminals.</summary>
    public MqttManualAcknowledgementRouteBuilder WithQualityOfService(MqttQualityOfService maximumQualityOfService)
    {
        _topicFilter = _topicFilter with { MaximumQualityOfService = maximumQualityOfService };
        return this;
    }

    /// <summary>Configures whether the broker should suppress this client's own publications.</summary>
    public MqttManualAcknowledgementRouteBuilder WithNoLocal(bool noLocal = true)
    {
        _topicFilter = _topicFilter with { NoLocal = noLocal };
        return this;
    }

    /// <summary>Configures whether retained deliveries keep their retain flag.</summary>
    public MqttManualAcknowledgementRouteBuilder WithRetainAsPublished(bool retainAsPublished = true)
    {
        _topicFilter = _topicFilter with { RetainAsPublished = retainAsPublished };
        return this;
    }

    /// <summary>Configures when retained messages are replayed for the broker subscription.</summary>
    public MqttManualAcknowledgementRouteBuilder WithRetainHandling(MqttRetainHandling retainHandling)
    {
        _topicFilter = _topicFilter with { RetainHandling = retainHandling };
        return this;
    }

    /// <summary>Bounds the route's queue. Manual acknowledgement routes are lossless and require <see cref="RouteOverflow.Wait"/>.</summary>
    public MqttManualAcknowledgementRouteBuilder WithQueue(int capacity, RouteOverflow overflow = RouteOverflow.Wait)
    {
        _options = _options with { Capacity = capacity, Overflow = overflow };
        return this;
    }

    /// <summary>Allows up to <paramref name="maxConcurrency"/> handler invocations at once. 1 preserves order.</summary>
    public MqttManualAcknowledgementRouteBuilder WithConcurrency(int maxConcurrency)
    {
        _options = _options with { MaxConcurrency = maxConcurrency };
        return this;
    }

    /// <summary>Subscribes the broker filter and registers a handler that explicitly acknowledges or rejects messages.</summary>
    public Task<MqttSubscribedRoute> HandleAsync(MqttManualAcknowledgementRouteHandler handler) =>
        HandleAsync(handler, CancellationToken.None);

    /// <summary>Subscribes the broker filter and registers a handler that explicitly acknowledges or rejects messages.</summary>
    public Task<MqttSubscribedRoute> HandleAsync(
        MqttManualAcknowledgementRouteHandler handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return ResilientMqttClientFluentExtensions.SubscribeRouteAsync(
            _client,
            _template,
            _topicFilter,
            () => _client.RegisterManualAcknowledgementRoute(_template, handler, _options),
            cancellationToken);
    }

    /// <summary>Subscribes the broker filter and opens a stream whose messages require explicit acknowledgement.</summary>
    public Task<MqttSubscribedAcknowledgedRouteStream> StreamAsync() =>
        StreamAsync(CancellationToken.None);

    /// <summary>Subscribes the broker filter and opens a stream whose messages require explicit acknowledgement.</summary>
    public Task<MqttSubscribedAcknowledgedRouteStream> StreamAsync(CancellationToken cancellationToken) =>
        ResilientMqttClientFluentExtensions.SubscribeAcknowledgedRouteStreamAsync(
            _client,
            _template,
            _topicFilter,
            () => _client.OpenAcknowledgedRouteStream(_template, _options),
            cancellationToken);
}
