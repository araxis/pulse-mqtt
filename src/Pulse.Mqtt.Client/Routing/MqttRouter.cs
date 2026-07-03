using System.Diagnostics;
using System.Threading.Channels;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>Handles one routed message.</summary>
public delegate ValueTask MqttRouteHandler(MqttPublishPacket message, MqttRouteValues values, CancellationToken cancellationToken);

/// <summary>
/// Dispatches received messages to registered route templates. Handler and stream routes each have
/// their own bounded queue and consumers, so a slow one does not stall other routes within its
/// capacity, and a lossy overflow policy (<see cref="RouteOverflow.DropOldest"/> /
/// <see cref="RouteOverflow.DropNewest"/>) isolates it entirely. Handler failures surface through
/// <see cref="HandlerFaulted"/> and never stop dispatch.
/// </summary>
/// <remarks>
/// Acknowledged route streams are the deliberate exception. They are lossless by construction — a
/// message is not released until the consumer acknowledges it, so they forbid the drop policies —
/// and they are delivered on the shared inbound sink so acknowledgement stays ordered against the
/// connection. A stalled acknowledged consumer therefore applies backpressure to the <em>whole
/// client</em> once its queue fills, not just to its own route: one MQTT connection multiplexes
/// every topic over one byte stream, and a PUBLISH must be read and parsed before its route is even
/// known, so there is no way to stop pulling one route's messages without stopping the socket. This
/// is intrinsic to lossless delivery over a single connection, not a defect. Two things keep it from
/// biting: size an acknowledged route's <see cref="MqttRouteOptions.Capacity"/> at or above the
/// client's advertised Receive Maximum (the client defaults it to that) so the broker's in-flight
/// window throttles before the queue fills; and a
/// <c>pulse.mqtt.client.route.acknowledged.backpressure</c> metric fires whenever a full acknowledged
/// route does block the sink. For true isolation between a latency-sensitive route and a slow
/// manual-ack one, use separate clients so each has its own connection.
/// </remarks>
public sealed class MqttRouter : IAsyncDisposable
{
    private readonly ChannelReader<MqttPublishPacket> _source;
    private readonly Channel<MqttPublishPacket> _unmatched;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _gate = new();
    private volatile Route[] _routes = [];
    private volatile AcknowledgedRoute[] _acknowledgedRoutes = [];
    private Task? _dispatch;
    private volatile bool _disposed;

    /// <summary>Creates a router that consumes <paramref name="source"/> once <see cref="Start"/> is called.</summary>
    public MqttRouter(ChannelReader<MqttPublishPacket> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        _source = source;
        _unmatched = Channel.CreateBounded<MqttPublishPacket>(new BoundedChannelOptions(64)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });
    }

    /// <summary>Messages that matched no registered route. Bounded; the oldest are dropped when unread.</summary>
    public ChannelReader<MqttPublishPacket> Unmatched => _unmatched.Reader;

    /// <summary>Raised when a route handler throws. Dispatch continues.</summary>
    public event Action<string, Exception>? HandlerFaulted;

    /// <summary>Starts consuming the source. Call exactly once.</summary>
    /// <exception cref="InvalidOperationException">The router is already started.</exception>
    public void Start()
    {
        if (_dispatch is not null)
        {
            throw new InvalidOperationException("The router is already started.");
        }

        _dispatch = Task.Run(() => DispatchAsync(_lifetime.Token), CancellationToken.None);
    }

    /// <summary>Registers a local handler for a route template. Dispose the registration to remove it.</summary>
    public IDisposable RegisterRoute(MqttRouteTemplate template, MqttRouteHandler handler) =>
        RegisterRoute(template, handler, options: null);

    /// <summary>Registers a local handler for a route template. Dispose the registration to remove it.</summary>
    public IDisposable RegisterRoute(MqttRouteTemplate template, MqttRouteHandler handler, MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(handler);
        ObjectDisposedException.ThrowIf(_disposed, this);

        return AddRoute(new Route(this, template, handler, options ?? new MqttRouteOptions(), _lifetime.Token));
    }

    /// <summary>Registers a local handler for a route template given as text.</summary>
    public IDisposable RegisterRoute(string template, MqttRouteHandler handler) =>
        RegisterRoute(MqttRouteTemplate.Parse(template), handler, options: null);

    /// <summary>Registers a local handler for a route template given as text.</summary>
    public IDisposable RegisterRoute(string template, MqttRouteHandler handler, MqttRouteOptions? options) =>
        RegisterRoute(MqttRouteTemplate.Parse(template), handler, options);

    /// <summary>Opens a consumable stream for a route template instead of registering a handler.</summary>
    public MqttRouteStream OpenRouteStream(MqttRouteTemplate template) =>
        OpenRouteStream(template, options: null);

    /// <summary>Opens a consumable stream for a route template instead of registering a handler.</summary>
    public MqttRouteStream OpenRouteStream(MqttRouteTemplate template, MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(template);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var route = new Route(this, template, handler: null, options ?? new MqttRouteOptions(), _lifetime.Token);
        var registration = AddRoute(route);
        return new MqttRouteStream(route.Reader, registration);
    }

    /// <summary>Opens a consumable stream for a route template given as text.</summary>
    public MqttRouteStream OpenRouteStream(string template) =>
        OpenRouteStream(MqttRouteTemplate.Parse(template), options: null);

    /// <summary>Opens a consumable stream for a route template given as text.</summary>
    public MqttRouteStream OpenRouteStream(string template, MqttRouteOptions? options) =>
        OpenRouteStream(MqttRouteTemplate.Parse(template), options);

    /// <summary>Opens a consumable stream with explicit protocol acknowledgement for a route template.</summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(MqttRouteTemplate template) =>
        OpenAcknowledgedRouteStream(template, options: null);

    /// <summary>Opens a consumable stream with explicit protocol acknowledgement for a route template.</summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(MqttRouteTemplate template, MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(template);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var routeOptions = options ?? new MqttRouteOptions();
        if (routeOptions.Overflow != RouteOverflow.Wait)
        {
            throw new ArgumentException(
                "Acknowledged route streams must use RouteOverflow.Wait because dropping a message would also drop its pending protocol acknowledgement.",
                nameof(options));
        }

        var route = new AcknowledgedRoute(template, routeOptions);
        var registration = AddAcknowledgedRoute(route);
        return new MqttAcknowledgedRouteStream(route.Reader, registration);
    }

    /// <summary>Opens a consumable stream with explicit protocol acknowledgement for a route template given as text.</summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(string template) =>
        OpenAcknowledgedRouteStream(MqttRouteTemplate.Parse(template), options: null);

    /// <summary>Opens a consumable stream with explicit protocol acknowledgement for a route template given as text.</summary>
    public MqttAcknowledgedRouteStream OpenAcknowledgedRouteStream(string template, MqttRouteOptions? options) =>
        OpenAcknowledgedRouteStream(MqttRouteTemplate.Parse(template), options);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _lifetime.CancelAsync().ConfigureAwait(false);

        if (_dispatch is { } dispatch)
        {
            await dispatch.ConfigureAwait(false);
        }

        foreach (var route in _routes)
        {
            await route.CompleteAsync().ConfigureAwait(false);
        }

        foreach (var route in _acknowledgedRoutes)
        {
            route.CompleteChannel();
        }

        _unmatched.Writer.TryComplete();
        _lifetime.Dispose();
    }

    internal async ValueTask<bool> DeliverAcknowledgedAsync(
        MqttInboundPublishContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        foreach (var route in _acknowledgedRoutes)
        {
            if (route.Template.TryMatch(context.Message.Topic, out var values))
            {
                await route.DeliverAsync(
                        new MqttAcknowledgedRoutedMessage(context, values),
                        cancellationToken)
                    .ConfigureAwait(false);
                return true;
            }
        }

        return false;
    }

    private IDisposable AddRoute(Route route)
    {
        lock (_gate)
        {
            _routes = [.. _routes, route];
        }

        return new Registration(this, route);
    }

    private void RemoveRoute(Route route)
    {
        lock (_gate)
        {
            _routes = _routes.Where(existing => !ReferenceEquals(existing, route)).ToArray();
        }

        _ = route.CompleteAsync();
    }

    private IDisposable AddAcknowledgedRoute(AcknowledgedRoute route)
    {
        lock (_gate)
        {
            _acknowledgedRoutes = [.. _acknowledgedRoutes, route];
        }

        return new AcknowledgedRegistration(this, route);
    }

    private void RemoveAcknowledgedRoute(AcknowledgedRoute route)
    {
        lock (_gate)
        {
            _acknowledgedRoutes = _acknowledgedRoutes.Where(existing => !ReferenceEquals(existing, route)).ToArray();
        }

        route.CompleteChannel();
    }

    private void RaiseHandlerFaulted(string template, Exception error) => HandlerFaulted?.Invoke(template, error);

    // Wraps each handler invocation in a Consumer span. When the message carries a W3C traceparent
    // (a producer with trace propagation enabled), the span is parented on the producer's span, so
    // the handler's work is a child across processes; otherwise it starts a fresh root.
    private static Activity? StartReceiveActivity(MqttPublishPacket message)
    {
        var activity = TraceContextPropagation.Extract(message.UserProperties) is { } remote
            ? PulseMqttDiagnostics.ActivitySource.StartActivity("receive", ActivityKind.Consumer, remote)
            : StartRootReceive();

        if (activity is not null)
        {
            activity.DisplayName = $"receive {message.Topic}";
            activity.SetTag("messaging.system", "mqtt");
            activity.SetTag("messaging.destination.name", message.Topic);
            activity.SetTag("messaging.operation.type", "process");
        }

        return activity;
    }

    // Starts a parentless receive span. A default ActivityContext still falls back to
    // Activity.Current, so the ambient is suppressed first — the consumer task captures whatever
    // Activity was current when the route was registered, and the receive span must not inherit it.
    private static Activity? StartRootReceive()
    {
        var previous = Activity.Current;
        Activity.Current = null;
        var activity = PulseMqttDiagnostics.ActivitySource.StartActivity("receive", ActivityKind.Consumer);
        if (activity is null)
        {
            Activity.Current = previous;
        }

        return activity;
    }

    private async Task DispatchAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _source.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_source.TryRead(out var message))
                {
                    var matched = false;
                    foreach (var route in _routes)
                    {
                        if (route.Template.TryMatch(message.Topic, out var values))
                        {
                            matched = true;
                            await route.DeliverAsync(new MqttRoutedMessage(message, values), cancellationToken).ConfigureAwait(false);
                        }
                    }

                    if (!matched)
                    {
                        _unmatched.Writer.TryWrite(message);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
        catch (ChannelClosedException)
        {
            // The source faulted; consumers observe their route channels completing below.
        }
        finally
        {
            foreach (var route in _routes)
            {
                route.CompleteChannel();
            }

            _unmatched.Writer.TryComplete();
        }
    }

    private sealed class Registration(MqttRouter router, Route route) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                router.RemoveRoute(route);
            }
        }
    }

    private sealed class AcknowledgedRegistration(MqttRouter router, AcknowledgedRoute route) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                router.RemoveAcknowledgedRoute(route);
            }
        }
    }

    private sealed class Route
    {
        private readonly MqttRouter _router;
        private readonly Channel<MqttRoutedMessage> _channel;
        private readonly Task[] _consumers;
        private readonly RouteOverflow _overflow;

        public Route(MqttRouter router, MqttRouteTemplate template, MqttRouteHandler? handler, MqttRouteOptions options, CancellationToken cancellationToken)
        {
            _router = router;
            Template = template;
            _overflow = options.Overflow;
            _channel = Channel.CreateBounded<MqttRoutedMessage>(new BoundedChannelOptions(options.Capacity)
            {
                SingleWriter = true,
                FullMode = options.Overflow switch
                {
                    RouteOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
                    RouteOverflow.DropNewest => BoundedChannelFullMode.DropWrite,
                    _ => BoundedChannelFullMode.Wait,
                },
            });

            _consumers = handler is null
                ? []
                : [.. Enumerable.Range(0, Math.Max(1, options.MaxConcurrency))
                    .Select(_ => Task.Run(() => ConsumeAsync(handler, cancellationToken), CancellationToken.None))];
        }

        public MqttRouteTemplate Template { get; }

        public ChannelReader<MqttRoutedMessage> Reader => _channel.Reader;

        public ValueTask DeliverAsync(MqttRoutedMessage message, CancellationToken cancellationToken)
        {
            if (_overflow == RouteOverflow.Wait)
            {
                return _channel.Writer.WriteAsync(message, cancellationToken);
            }

            _channel.Writer.TryWrite(message); // drop modes always accept
            return ValueTask.CompletedTask;
        }

        public void CompleteChannel() => _channel.Writer.TryComplete();

        public async Task CompleteAsync()
        {
            CompleteChannel();
            foreach (var consumer in _consumers)
            {
                await consumer.ConfigureAwait(false);
            }
        }

        private async Task ConsumeAsync(MqttRouteHandler handler, CancellationToken cancellationToken)
        {
            try
            {
                while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (_channel.Reader.TryRead(out var routed))
                    {
                        using var activity = StartReceiveActivity(routed.Message);
                        try
                        {
                            await handler(routed.Message, routed.Values, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception error)
                        {
                            activity?.SetStatus(ActivityStatusCode.Error, error.Message);
                            _router.RaiseHandlerFaulted(Template.Template, error);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Shutdown.
            }
        }
    }

    private sealed class AcknowledgedRoute
    {
        private readonly Channel<MqttAcknowledgedRoutedMessage> _channel;
        private readonly RouteOverflow _overflow;

        public AcknowledgedRoute(MqttRouteTemplate template, MqttRouteOptions options)
        {
            Template = template;
            _overflow = options.Overflow;
            _channel = Channel.CreateBounded<MqttAcknowledgedRoutedMessage>(new BoundedChannelOptions(options.Capacity)
            {
                SingleWriter = true,
                FullMode = options.Overflow switch
                {
                    RouteOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
                    RouteOverflow.DropNewest => BoundedChannelFullMode.DropWrite,
                    _ => BoundedChannelFullMode.Wait,
                },
            });
        }

        public MqttRouteTemplate Template { get; }

        public ChannelReader<MqttAcknowledgedRoutedMessage> Reader => _channel.Reader;

        public async ValueTask DeliverAsync(MqttAcknowledgedRoutedMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (_overflow == RouteOverflow.Wait)
                {
                    // A non-blocking write while there is room keeps the sink flowing. Only when the
                    // queue is full does the lossless contract force the sink to wait — record that
                    // this route is applying backpressure to the whole client before blocking on it.
                    if (!_channel.Writer.TryWrite(message))
                    {
                        PulseMqttDiagnostics.AcknowledgedRouteBackpressure.Add(
                            1, new KeyValuePair<string, object?>("route", Template.Template));
                        await _channel.Writer.WriteAsync(message, cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

                _channel.Writer.TryWrite(message);
            }
            catch (ChannelClosedException)
            {
                // The stream was disposed while this lossless delivery was pending on the shared
                // inbound sink. Treat the route as gone — this exception must never escape into the
                // sink, where it would fault the read loop and tear down the whole connection.
            }
        }

        public void CompleteChannel() => _channel.Writer.TryComplete();
    }
}
