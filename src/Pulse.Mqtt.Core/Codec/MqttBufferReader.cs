using System.Buffers.Binary;
using System.Text;

namespace Pulse.Mqtt.Codec;

/// <summary>
/// A forward-only reader over a complete MQTT packet buffer. Reads the protocol's primitive wire
/// types (integers, variable-length integers, length-prefixed UTF-8 strings and binary data).
/// The reader is a <see langword="ref struct"/> and copies only when materializing a string.
/// Truncated or invalid input raises <see cref="MqttProtocolException"/>.
/// </summary>
public ref struct MqttBufferReader
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly ReadOnlySpan<byte> _buffer;
    private int _position;

    /// <summary>Creates a reader over <paramref name="buffer"/>.</summary>
    public MqttBufferReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    /// <summary>Number of bytes consumed so far.</summary>
    public readonly int Consumed => _position;

    /// <summary>Number of bytes remaining.</summary>
    public readonly int Remaining => _buffer.Length - _position;

    /// <summary>Reads a single byte.</summary>
    public byte ReadByte()
    {
        EnsureAvailable(1);
        return _buffer[_position++];
    }

    /// <summary>Reads a big-endian unsigned 16-bit integer.</summary>
    public ushort ReadUInt16()
    {
        EnsureAvailable(2);
        var value = BinaryPrimitives.ReadUInt16BigEndian(_buffer.Slice(_position, 2));
        _position += 2;
        return value;
    }

    /// <summary>Reads a big-endian unsigned 32-bit integer.</summary>
    public uint ReadUInt32()
    {
        EnsureAvailable(4);
        var value = BinaryPrimitives.ReadUInt32BigEndian(_buffer.Slice(_position, 4));
        _position += 4;
        return value;
    }

    /// <summary>Reads a variable-length integer.</summary>
    /// <exception cref="MqttProtocolException">The integer is truncated or malformed.</exception>
    public uint ReadVarInt()
    {
        var status = MqttVarInt.TryRead(_buffer[_position..], out var value, out var bytesRead);
        if (status != MqttVarIntStatus.Success)
        {
            throw new MqttProtocolException($"Invalid variable-length integer ({status}).");
        }

        _position += bytesRead;
        return value;
    }

    /// <summary>Reads <paramref name="length"/> raw bytes as a slice of the underlying buffer (no copy).</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative.</exception>
    /// <exception cref="MqttProtocolException">Fewer than <paramref name="length"/> bytes remain.</exception>
    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        if (length < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, "Length must be non-negative.");
        }

        EnsureAvailable(length);
        var slice = _buffer.Slice(_position, length);
        _position += length;
        return slice;
    }

    /// <summary>Reads length-prefixed binary data as a slice of the underlying buffer (no copy).</summary>
    /// <exception cref="MqttProtocolException">The declared length runs past the buffer.</exception>
    public ReadOnlySpan<byte> ReadBinary()
    {
        int length = ReadUInt16();
        EnsureAvailable(length);
        var slice = _buffer.Slice(_position, length);
        _position += length;
        return slice;
    }

    /// <summary>Reads a length-prefixed UTF-8 string.</summary>
    /// <exception cref="MqttProtocolException">The string is truncated or not valid UTF-8.</exception>
    public string ReadString()
    {
        var bytes = ReadBinary();
        if (bytes.IsEmpty)
        {
            return string.Empty;
        }

        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException ex)
        {
            throw new MqttProtocolException("String is not valid UTF-8.", ex);
        }
    }

    private readonly void EnsureAvailable(int count)
    {
        if (count > Remaining)
        {
            throw new MqttProtocolException($"Unexpected end of packet: needed {count} byte(s) but {Remaining} remain.");
        }
    }
}
