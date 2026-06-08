using System.Buffers;

namespace Pulse.Mqtt.Codec;

/// <summary>Writes MQTT fixed headers into an <see cref="IBufferWriter{T}"/>.</summary>
public static class MqttFrameWriter
{
    /// <summary>Writes the fixed header: the packed first byte followed by the remaining length as a variable integer.</summary>
    /// <param name="output">The destination buffer writer.</param>
    /// <param name="header">The fixed header to encode.</param>
    /// <exception cref="ArgumentOutOfRangeException">The remaining length is negative or exceeds the protocol maximum.</exception>
    public static void WriteHeader(IBufferWriter<byte> output, MqttFixedHeader header)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (header.RemainingLength is < 0 or > MqttVarInt.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(header),
                header.RemainingLength,
                "Remaining length must be between 0 and the MQTT variable integer maximum.");
        }

        var writer = new MqttBufferWriter(output);
        writer.WriteByte(header.FirstByte);
        writer.WriteVarInt((uint)header.RemainingLength);
    }
}
