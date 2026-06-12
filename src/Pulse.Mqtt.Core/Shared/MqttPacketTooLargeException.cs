namespace Pulse.Mqtt;

/// <summary>
/// Raised when an outbound packet's encoded size exceeds the maximum packet size the broker
/// advertised in its CONNACK. The packet never reaches the wire — failing here beats being
/// disconnected by the broker with <c>PacketTooLarge</c>.
/// </summary>
public sealed class MqttPacketTooLargeException : MqttException
{
    /// <summary>Creates the exception from the offending size and the negotiated limit.</summary>
    public MqttPacketTooLargeException(int packetSize, uint limit)
        : base($"The encoded packet is {packetSize} bytes; the broker accepts at most {limit}.")
    {
        PacketSize = packetSize;
        Limit = limit;
    }

    /// <summary>The encoded size of the rejected packet, fixed header included.</summary>
    public int PacketSize { get; }

    /// <summary>The broker's advertised maximum packet size.</summary>
    public uint Limit { get; }
}
