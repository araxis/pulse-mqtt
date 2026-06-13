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

    /// <summary>
    /// The connection dropped mid-exchange while a persistent session was tracking this QoS 1/2
    /// publish. The session holds it and redelivers it (with DUP) <em>if the broker preserves the
    /// session</em>. If the broker reports a fresh session on reconnect, the held message is
    /// discarded per the specification (a <c>Warning</c> log records how many) — so
    /// <see cref="InFlight"/> is "held for redelivery on resume," not an unconditional guarantee.
    /// </summary>
    InFlight,
}

/// <summary>The result of a resilient publish.</summary>
/// <param name="Disposition">Whether the message was delivered, queued, or dropped.</param>
/// <param name="ReasonCode">The broker's reason code when delivered; <see cref="MqttReasonCode.Success"/> otherwise.</param>
public readonly record struct PublishOutcome(PublishDisposition Disposition, MqttReasonCode ReasonCode = MqttReasonCode.Success);
