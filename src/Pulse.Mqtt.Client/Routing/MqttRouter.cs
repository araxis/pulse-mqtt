using System.Diagnostics;
using System.Threading.Channels;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>Handles one routed message.</summary>
public delegate ValueTask MqttRouteHandler(MqttPublishPacket message, MqttRouteValues values, CancellationToken cancellationToken);

/// <summary>
/// Dispatches received messages to registered route templates. Each route has its own bounded
/// queue and consumers, so a slow handler does not stall other routes (within its queue capacity).
/// Handler failures surface through <see cref="HandlerFaulted"/> and never stop dispatch.
/// </summary>
public sealed class MqttRouter : IAsyncDisposable
{
    private readonly ChannelReader<MqttPublishPacket> _source;
    private readonly Channel<MqttPublishPacket> _unmatched;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _gate = new();
    private volatile Route[] _routes = [];
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

    /// <summary>Registers a handler for a route template. Dispose the registration to remove it.</summary>
    public IDisposable On(MqttRouteTemplate template, MqttRouteHandler handler, MqttRouteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(handler);
        ObjectDisposedException.ThrowIf(_disposed, this);

        return AddRoute(new Route(this, template, handler, options ?? new MqttRouteOptions(), _lifetime.Token));
    }

    /// <summary>Registers a handler for a route template given as text.</summary>
    public IDisposable On(string template, MqttRouteHandler handler, MqttRouteOptions? options = null) =>
        On(MqttRouteTemplate.Parse(template), handler, options);

    /// <summary>Opens a consumable stream for a route template instead of registering a handler.</summary>
    public MqttRouteStream OpenStream(MqttRouteTemplate template, MqttRouteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(template);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var route = new Route(this, template, handler: null, options ?? new MqttRouteOptions(), _lifetime.Token);
        var registration = AddRoute(route);
        return new MqttRouteStream(route.Reader, registration);
    }

    /// <summary>Opens a consumable stream for a route template given as text.</summary>
    public MqttRouteStream OpenStream(string template, MqttRouteOptions? options = null) =>
        OpenStream(MqttRouteTemplate.Parse(template), options);

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

        _unmatched.Writer.TryComplete();
        _lifetime.Dispose();
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
}
