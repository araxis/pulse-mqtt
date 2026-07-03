using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// A publish waiting in the offline queue, together with when it entered the queue.
/// <see cref="EnqueuedAt"/> is <see langword="null"/> for stores (or persisted rows) that predate
/// queue-time tracking; such messages are flushed as-is, without expiry accounting.
/// </summary>
public sealed record MqttQueuedPublish(MqttPublishPacket Packet, DateTimeOffset? EnqueuedAt);
