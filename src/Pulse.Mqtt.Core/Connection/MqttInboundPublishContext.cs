using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Connection;

/// <summary>An inbound PUBLISH plus the protocol acknowledgement for that delivery.</summary>
public sealed class MqttInboundPublishContext
{
    private readonly Func<MqttReasonCode, string?, CancellationToken, ValueTask> _complete;
    private int _completed;

    internal MqttInboundPublishContext(
        MqttPublishPacket message,
        bool canReject,
        Func<MqttReasonCode, string?, CancellationToken, ValueTask> complete)
    {
        Message = message ?? throw new ArgumentNullException(nameof(message));
        CanReject = canReject;
        _complete = complete ?? throw new ArgumentNullException(nameof(complete));
    }

    /// <summary>The inbound application message.</summary>
    public MqttPublishPacket Message { get; }

    /// <summary>Whether this delivery can send a protocol-level negative acknowledgement.</summary>
    public bool CanReject { get; }

    /// <summary>Whether this context has already sent, or skipped, its protocol acknowledgement.</summary>
    public bool IsAcknowledged => Volatile.Read(ref _completed) != 0;

    /// <summary>Acknowledges successful processing of the inbound message.</summary>
    public ValueTask AcknowledgeAsync(CancellationToken cancellationToken = default) =>
        CompleteAsync(MqttReasonCode.Success, reasonString: null, cancellationToken);

    /// <summary>Rejects the inbound message with an MQTT 5 failure reason where the protocol can carry one.</summary>
    public ValueTask RejectAsync(
        MqttReasonCode reasonCode = MqttReasonCode.UnspecifiedError,
        string? reasonString = null,
        CancellationToken cancellationToken = default)
    {
        if (!CanReject)
        {
            throw new NotSupportedException(
                "This inbound publish cannot be rejected at the MQTT protocol level. MQTT 5 QoS 1/2 deliveries support negative acknowledgement; MQTT 3.1.1 and QoS 0 deliveries do not.");
        }

        if ((byte)reasonCode < 0x80)
        {
            throw new ArgumentOutOfRangeException(
                nameof(reasonCode),
                reasonCode,
                "A rejected inbound publish must use an MQTT failure reason code.");
        }

        return CompleteAsync(reasonCode, reasonString, cancellationToken);
    }

    private ValueTask CompleteAsync(
        MqttReasonCode reasonCode,
        string? reasonString,
        CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return ValueTask.CompletedTask;
        }

        return _complete(reasonCode, reasonString, cancellationToken);
    }
}
