namespace Pulse.Mqtt.Resilience;

/// <summary>Settings for the offline outbound queue. Everything is bounded and explicit.</summary>
public sealed record OfflineQueueOptions
{
    /// <summary>The maximum number of queued publishes.</summary>
    public int Capacity { get; init; } = 1024;

    /// <summary>What happens when the queue is full. Defaults to <see cref="OverflowPolicy.Block"/>.</summary>
    public OverflowPolicy Overflow { get; init; } = OverflowPolicy.Block;

    /// <summary>
    /// Whether QoS 0 publishes are queued while offline. QoS 0 is best-effort, so the default is
    /// to drop them (counted, never silent).
    /// </summary>
    public bool IncludeQos0 { get; init; }

    /// <summary>
    /// How long a <see cref="OverflowPolicy.Block"/> publish waits for space before failing with
    /// <see cref="OfflineQueueFullException"/>. <see langword="null"/> waits indefinitely.
    /// </summary>
    public TimeSpan? PublishWaitTimeout { get; init; }
}
