namespace Pulse.Mqtt;

/// <summary>
/// Raised when the connection dies in the middle of a QoS 1/2 exchange that a persistent
/// session is tracking. The message is <em>not</em> lost and must not be re-published: the
/// session holds it and redelivers it (with DUP) when the broker resumes the session.
/// </summary>
public sealed class MqttPublishInFlightException : MqttException
{
    /// <summary>Creates the exception for the interrupted exchange.</summary>
    public MqttPublishInFlightException(ushort packetIdentifier, Exception innerException)
        : base($"The connection closed mid-exchange; packet {packetIdentifier} stays in flight and redelivers on session resume.", innerException)
    {
        PacketIdentifier = packetIdentifier;
    }

    /// <summary>The packet identifier the session holds.</summary>
    public ushort PacketIdentifier { get; }
}
