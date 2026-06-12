namespace Pulse.Mqtt.Resilience;

/// <summary>Raised when the offline queue is full and the overflow policy fails the publish.</summary>
public sealed class OfflineQueueFullException : MqttException
{
    /// <summary>Initializes a new instance for a queue of <paramref name="capacity"/> messages.</summary>
    public OfflineQueueFullException(int capacity)
        : base($"The offline queue is full ({capacity} messages).")
    {
    }
}
