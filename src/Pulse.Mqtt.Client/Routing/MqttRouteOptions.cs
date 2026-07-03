namespace Pulse.Mqtt.Client;

/// <summary>What happens when a route's queue is full.</summary>
public enum RouteOverflow
{
    /// <summary>The dispatcher waits for space — lossless, but backpressure reaches other routes once full.</summary>
    Wait,

    /// <summary>The oldest queued message is evicted — lossy, but a slow route never affects others.</summary>
    DropOldest,

    /// <summary>The incoming message is dropped — lossy, but a slow route never affects others.</summary>
    DropNewest,
}

/// <summary>Per-route delivery settings.</summary>
public sealed record MqttRouteOptions
{
    /// <summary>
    /// The bounded capacity of the route's queue. For an acknowledged route stream, size this at or
    /// above the client's advertised Receive Maximum so the broker's in-flight window throttles
    /// before the queue fills and blocks the shared inbound sink; the client defaults it to that
    /// value when you open an acknowledged stream without explicit options.
    /// </summary>
    public int Capacity { get; init; } = 64;

    /// <summary>The overflow behavior when the queue is full. Defaults to <see cref="RouteOverflow.Wait"/>.</summary>
    public RouteOverflow Overflow { get; init; } = RouteOverflow.Wait;

    /// <summary>How many handler invocations may run concurrently. 1 (the default) preserves order.</summary>
    public int MaxConcurrency { get; init; } = 1;

}
