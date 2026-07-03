using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// The bounded store for publishes queued while offline. This is a swap point: the in-memory
/// default lives for the process; durable implementations survive restarts. The flush loop uses
/// peek/remove-head so a crash between the two re-sends rather than loses (at-least-once).
/// </summary>
public interface IMessageStore
{
    /// <summary>The number of queued publishes.</summary>
    int Count { get; }

    /// <summary>How many publishes the overflow policy has dropped. Never silent.</summary>
    long DroppedCount { get; }

    /// <summary>
    /// Queues a publish, applying the configured overflow policy when full: blocks, evicts the
    /// oldest, drops the newest, or throws <see cref="OfflineQueueFullException"/>.
    /// </summary>
    ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken);

    /// <summary>
    /// Queues a publish stamped with when it entered the queue, so message expiry can be
    /// honored across the offline wait. The default implementation forwards to
    /// <see cref="EnqueueAsync(MqttPublishPacket, CancellationToken)"/> and loses the stamp,
    /// which disables expiry accounting for stores that do not override it.
    /// </summary>
    ValueTask EnqueueAsync(MqttPublishPacket packet, DateTimeOffset enqueuedAt, CancellationToken cancellationToken) =>
        EnqueueAsync(packet, cancellationToken);

    /// <summary>Returns the oldest queued publish without removing it, or <see langword="null"/> when empty.</summary>
    ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the oldest queued publish with its enqueue time, or <see langword="null"/> when
    /// empty. The default implementation wraps <see cref="PeekAsync"/> with an unknown enqueue
    /// time, which disables expiry accounting for stores that do not override it.
    /// </summary>
    async ValueTask<MqttQueuedPublish?> PeekQueuedAsync(CancellationToken cancellationToken) =>
        await PeekAsync(cancellationToken).ConfigureAwait(false) is { } head
            ? new MqttQueuedPublish(head, EnqueuedAt: null)
            : null;

    /// <summary>Removes the oldest queued publish after it was successfully flushed.</summary>
    ValueTask RemoveHeadAsync(CancellationToken cancellationToken);

    /// <summary>Removes all queued publishes.</summary>
    ValueTask ClearAsync(CancellationToken cancellationToken);
}
