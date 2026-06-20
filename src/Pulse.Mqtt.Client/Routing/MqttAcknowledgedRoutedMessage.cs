using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>A routed inbound message with explicit protocol acknowledgement control.</summary>
public sealed class MqttAcknowledgedRoutedMessage
{
    private readonly MqttInboundPublishContext _context;

    internal MqttAcknowledgedRoutedMessage(
        MqttInboundPublishContext context,
        MqttRouteValues values)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        Values = values;
    }

    /// <summary>The received PUBLISH.</summary>
    public MqttPublishPacket Message => _context.Message;

    /// <summary>The parameter values the route template captured.</summary>
    public MqttRouteValues Values { get; }

    /// <summary>Whether this message has already sent, or skipped, its protocol acknowledgement.</summary>
    public bool IsAcknowledged => _context.IsAcknowledged;

    /// <summary>Whether this delivery can send a protocol-level negative acknowledgement.</summary>
    public bool CanReject => _context.CanReject;

    /// <summary>Acknowledges successful processing of the inbound message.</summary>
    public ValueTask AcknowledgeAsync(CancellationToken cancellationToken = default) =>
        _context.AcknowledgeAsync(cancellationToken);

    /// <summary>Rejects the inbound message with an MQTT 5 failure reason where the protocol can carry one.</summary>
    public ValueTask RejectAsync(
        MqttReasonCode reasonCode = MqttReasonCode.UnspecifiedError,
        string? reasonString = null,
        CancellationToken cancellationToken = default) =>
        _context.RejectAsync(reasonCode, reasonString, cancellationToken);
}
