namespace Pulse.Mqtt.Resilience;

/// <summary>What happens when a publish arrives and the offline queue is full.</summary>
public enum OverflowPolicy
{
    /// <summary>The publisher waits until space frees up (bounded backpressure to the caller).</summary>
    Block,

    /// <summary>The oldest queued message is evicted to make room for the new one.</summary>
    DropOldest,

    /// <summary>The new message is dropped; queued messages are untouched.</summary>
    DropNewest,

    /// <summary>The publish fails immediately with <see cref="OfflineQueueFullException"/>.</summary>
    Reject,
}
