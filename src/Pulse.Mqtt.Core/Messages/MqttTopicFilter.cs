namespace Pulse.Mqtt;

/// <summary>
/// A topic filter for a subscription, together with its MQTT 5 subscription options
/// (maximum QoS, no-local, retain-as-published, and retain handling).
/// </summary>
/// <param name="Topic">The topic filter, which may contain the <c>+</c> and <c>#</c> wildcards.</param>
public sealed record MqttTopicFilter(string Topic)
{
    /// <summary>The maximum QoS at which the broker may deliver matching messages.</summary>
    public MqttQualityOfService MaximumQualityOfService { get; init; } = MqttQualityOfService.AtMostOnce;

    /// <summary>When set, the broker does not echo the subscriber's own publications back to it.</summary>
    public bool NoLocal { get; init; }

    /// <summary>When set, forwarded messages keep the RETAIN flag they were published with.</summary>
    public bool RetainAsPublished { get; init; }

    /// <summary>Controls when retained messages are sent for this subscription.</summary>
    public MqttRetainHandling RetainHandling { get; init; } = MqttRetainHandling.SendAtSubscribe;

    /// <summary>Packs the filter's options into the single MQTT 5 subscription options byte.</summary>
    public byte ToSubscriptionOptions() => (byte)(
        (byte)MaximumQualityOfService
        | (NoLocal ? 1 << 2 : 0)
        | (RetainAsPublished ? 1 << 3 : 0)
        | ((byte)RetainHandling << 4));

    /// <summary>Reconstructs a filter from a topic and the MQTT 5 subscription options byte.</summary>
    /// <param name="topic">The topic filter.</param>
    /// <param name="options">The subscription options byte.</param>
    public static MqttTopicFilter FromSubscriptionOptions(string topic, byte options) => new(topic)
    {
        MaximumQualityOfService = (MqttQualityOfService)(options & 0x03),
        NoLocal = (options & (1 << 2)) != 0,
        RetainAsPublished = (options & (1 << 3)) != 0,
        RetainHandling = (MqttRetainHandling)((options >> 4) & 0x03),
    };
}
