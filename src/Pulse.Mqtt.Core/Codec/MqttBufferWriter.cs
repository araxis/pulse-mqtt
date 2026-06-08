using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Pulse.Mqtt.Codec;

/// <summary>
/// Writes MQTT primitive wire types into an <see cref="IBufferWriter{T}"/>. Output spans are
/// rented from the writer, so encoding allocates no intermediate arrays.
/// </summary>
public readonly ref struct MqttBufferWriter
{
    private readonly IBufferWriter<byte> _output;

    /// <summary>Creates a writer over <paramref name="output"/>.</summary>
    public MqttBufferWriter(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        _output = output;
    }

    /// <summary>Writes a single byte.</summary>
    public void WriteByte(byte value)
    {
        var span = _output.GetSpan(1);
        span[0] = value;
        _output.Advance(1);
    }

    /// <summary>Writes a big-endian unsigned 16-bit integer.</summary>
    public void WriteUInt16(ushort value)
    {
        var span = _output.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        _output.Advance(2);
    }

    /// <summary>Writes a big-endian unsigned 32-bit integer.</summary>
    public void WriteUInt32(uint value)
    {
        var span = _output.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        _output.Advance(4);
    }

    /// <summary>Writes a variable-length integer.</summary>
    public void WriteVarInt(uint value)
    {
        var span = _output.GetSpan(MqttVarInt.MaxEncodedLength);
        var written = MqttVarInt.Write(span, value);
        _output.Advance(written);
    }

    /// <summary>Writes length-prefixed binary data.</summary>
    /// <exception cref="ArgumentException"><paramref name="value"/> exceeds the 65,535-byte protocol limit.</exception>
    public void WriteBinary(ReadOnlySpan<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentException($"Binary data exceeds the {ushort.MaxValue}-byte protocol limit.", nameof(value));
        }

        WriteUInt16((ushort)value.Length);
        if (value.IsEmpty)
        {
            return;
        }

        var span = _output.GetSpan(value.Length);
        value.CopyTo(span);
        _output.Advance(value.Length);
    }

    /// <summary>Writes a length-prefixed UTF-8 string.</summary>
    /// <exception cref="ArgumentException"><paramref name="value"/> encodes to more than 65,535 bytes.</exception>
    public void WriteString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount > ushort.MaxValue)
        {
            throw new ArgumentException($"String exceeds the {ushort.MaxValue}-byte protocol limit.", nameof(value));
        }

        WriteUInt16((ushort)byteCount);
        if (byteCount == 0)
        {
            return;
        }

        var span = _output.GetSpan(byteCount);
        Encoding.UTF8.GetBytes(value, span);
        _output.Advance(byteCount);
    }
}
