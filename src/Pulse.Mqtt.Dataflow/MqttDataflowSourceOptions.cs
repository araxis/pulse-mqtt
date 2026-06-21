namespace Pulse.Mqtt.Dataflow;

/// <summary>Options for MQTT-backed Dataflow source blocks.</summary>
public sealed class MqttDataflowSourceOptions
{
    /// <summary>The bounded capacity of the source block's output buffer.</summary>
    public int BoundedCapacity { get; init; } = 256;

    internal static int Validate(MqttDataflowSourceOptions? options)
    {
        var capacity = (options ?? new MqttDataflowSourceOptions()).BoundedCapacity;
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BoundedCapacity),
                capacity,
                "BoundedCapacity must be positive.");
        }

        return capacity;
    }
}
