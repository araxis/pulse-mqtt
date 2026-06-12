using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Client;

/// <summary>What happened to a publish handed to the resilient client.</summary>
public enum PublishDisposition
{
    /// <summary>The broker received the message (QoS &gt; 0: acknowledged).</summary>
    Delivered,

    /// <summary>The client is offline; the message is queued and flushes on reconnect.</summary>
    Queued,

    /// <summary>The client is offline and QoS 0 messages are configured to drop.</summary>
    DroppedOffline,
}

/// <summary>The result of a resilient publish.</summary>
/// <param name="Disposition">Whether the message was delivered, queued, or dropped.</param>
/// <param name="ReasonCode">The broker's reason code when delivered; <see cref="MqttReasonCode.Success"/> otherwise.</param>
public readonly record struct PublishOutcome(PublishDisposition Disposition, MqttReasonCode ReasonCode = MqttReasonCode.Success);
