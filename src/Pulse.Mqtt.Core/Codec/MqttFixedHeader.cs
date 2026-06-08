namespace Pulse.Mqtt.Codec;

/// <summary>
/// The MQTT fixed header: a control packet type, four type-specific flag bits, and the remaining
/// length (the byte count of the variable header plus payload that follows the fixed header).
/// </summary>
/// <param name="PacketType">The control packet type.</param>
/// <param name="Flags">The low four flag bits specific to the packet type.</param>
/// <param name="RemainingLength">Number of bytes in the rest of the packet (variable header + payload).</param>
public readonly record struct MqttFixedHeader(MqttPacketType PacketType, byte Flags, int RemainingLength)
{
    /// <summary>The encoded first byte: packet type in the high nibble, flags in the low nibble.</summary>
    public byte FirstByte => (byte)(((byte)PacketType << 4) | (Flags & 0x0F));
}
