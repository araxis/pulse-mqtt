using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// A publish waiting in the offline queue, together with when it entered the queue.
/// <see cref="EnqueuedAt"/> is <see langword="null"/> for stores (or persisted rows) that predate
/// queue-time tracking; such messages are flushed as-is, without expiry accounting.
/// </summary>
public sealed record MqttQueuedPublish(MqttPublishPacket Packet, DateTimeOffset? EnqueuedAt)
{
    /// <summary>
    /// A store-assigned identity for this queued entry, set by <c>PeekQueuedAsync</c>. The flush
    /// passes it to <see cref="IMessageStore.RemoveAsync"/> so the entry it sent is removed even if
    /// the queue head changed meanwhile (for example a <see cref="OverflowPolicy.DropOldest"/>
    /// eviction). <c>0</c> means the store does not track identities, and removal falls back to the
    /// head. Not persisted — it is only meaningful within one store instance.
    /// </summary>
    public long Sequence { get; init; }
}
