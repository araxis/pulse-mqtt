namespace Pulse.Mqtt.Codec;

/// <summary>Outcome of attempting to read a variable-length integer from a byte span.</summary>
public enum MqttVarIntStatus
{
    /// <summary>A complete value was read.</summary>
    Success,

    /// <summary>The span ended before the value completed; read again once more bytes arrive.</summary>
    Incomplete,

    /// <summary>The encoding was malformed (it would require more than four bytes).</summary>
    Malformed,
}

/// <summary>
/// Encodes and decodes the MQTT "variable byte integer" used for the remaining length and several
/// MQTT 5 properties. Values range 0..268,435,455 and occupy one to four bytes — seven data bits
/// per byte, with the high bit marking continuation. All operations are allocation-free.
/// </summary>
public static class MqttVarInt
{
    /// <summary>Maximum number of bytes a value occupies on the wire.</summary>
    public const int MaxEncodedLength = 4;

    /// <summary>Largest representable value (0x0FFF_FFFF).</summary>
    public const int MaxValue = 268_435_455;

    /// <summary>Returns the number of bytes required to encode <paramref name="value"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> exceeds <see cref="MaxValue"/>.</exception>
    public static int GetEncodedLength(uint value)
    {
        ThrowIfTooLarge(value);

        return value switch
        {
            < 0x80 => 1,
            < 0x4000 => 2,
            < 0x20_0000 => 3,
            _ => 4,
        };
    }

    /// <summary>Writes <paramref name="value"/> into <paramref name="destination"/> and returns the byte count.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> exceeds <see cref="MaxValue"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too small for the encoded value.</exception>
    public static int Write(Span<byte> destination, uint value)
    {
        ThrowIfTooLarge(value);

        var index = 0;
        do
        {
            if (index >= destination.Length)
            {
                throw new ArgumentException("Destination is too small for the encoded value.", nameof(destination));
            }

            var encoded = (byte)(value & 0x7F);
            value >>= 7;
            if (value > 0)
            {
                encoded |= 0x80;
            }

            destination[index++] = encoded;
        }
        while (value > 0);

        return index;
    }

    /// <summary>Attempts to read a variable-length integer from the start of <paramref name="source"/>.</summary>
    /// <param name="source">Bytes to read from.</param>
    /// <param name="value">The decoded value on success; otherwise zero.</param>
    /// <param name="bytesRead">The number of bytes consumed on success; otherwise zero.</param>
    public static MqttVarIntStatus TryRead(ReadOnlySpan<byte> source, out uint value, out int bytesRead)
    {
        uint result = 0;
        var multiplier = 1u;

        for (var i = 0; i < MaxEncodedLength; i++)
        {
            if (i >= source.Length)
            {
                value = 0;
                bytesRead = 0;
                return MqttVarIntStatus.Incomplete;
            }

            var encoded = source[i];
            result += (encoded & 0x7Fu) * multiplier;

            if ((encoded & 0x80) == 0)
            {
                value = result;
                bytesRead = i + 1;
                return MqttVarIntStatus.Success;
            }

            multiplier <<= 7;
        }

        value = 0;
        bytesRead = 0;
        return MqttVarIntStatus.Malformed;
    }

    private static void ThrowIfTooLarge(uint value)
    {
        if (value > MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Value exceeds the MQTT variable integer maximum.");
        }
    }
}
