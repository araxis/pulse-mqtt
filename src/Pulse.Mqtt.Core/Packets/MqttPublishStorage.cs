namespace Pulse.Mqtt.Packets;

/// <summary>
/// Adapts a PUBLISH between its live form and the form a durable store persists. A QoS &gt; 0
/// publish awaiting its first send has no packet identifier yet — the client assigns one at send
/// time — but the wire codec durable stores reuse requires two identifier bytes for QoS &gt; 0.
/// The reserved identifier <c>0</c> (never a valid assigned identifier; those are 1..65535) stands
/// in for "not yet assigned" on disk and is restored to <see langword="null"/> on load. This lets
/// an offline QoS 1/2 publish round-trip through a durable offline-queue store, while an in-flight
/// session packet — which always carries a real identifier — passes through untouched. Custom
/// <c>IMessageStore</c>/<c>ISessionStore</c> implementations that serialize packets should apply
/// the same adaptation.
/// </summary>
public static class MqttPublishStorage
{
    private const ushort Unassigned = 0;

    /// <summary>
    /// Returns the packet in the form to persist: a QoS &gt; 0 publish with no identifier gets the
    /// reserved <c>0</c> placeholder so it can be wire-encoded; every other packet is unchanged.
    /// </summary>
    public static MqttPublishPacket ToStorageForm(MqttPublishPacket packet)
    {
        ArgumentNullException.ThrowIfNull(packet);
        return packet.QualityOfService != global::Pulse.Mqtt.MqttQualityOfService.AtMostOnce && packet.PacketIdentifier is null
            ? packet with { PacketIdentifier = Unassigned }
            : packet;
    }

    /// <summary>
    /// Reverses <see cref="ToStorageForm"/>: a QoS &gt; 0 publish carrying the reserved <c>0</c>
    /// placeholder is restored to a null identifier (to be assigned at send time); every other
    /// packet — including an in-flight session packet with a real identifier — is unchanged.
    /// </summary>
    public static MqttPublishPacket FromStorageForm(MqttPublishPacket packet)
    {
        ArgumentNullException.ThrowIfNull(packet);
        return packet.QualityOfService != global::Pulse.Mqtt.MqttQualityOfService.AtMostOnce && packet.PacketIdentifier == Unassigned
            ? packet with { PacketIdentifier = null }
            : packet;
    }
}
