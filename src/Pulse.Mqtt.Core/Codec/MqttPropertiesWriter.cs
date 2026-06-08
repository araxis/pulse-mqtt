using System.Buffers;

namespace Pulse.Mqtt.Codec;

/// <summary>
/// Writes typed MQTT 5 properties into a buffer. Because the property section is length-prefixed,
/// write the properties into a scratch buffer (for example a
/// <see cref="Pulse.Mqtt.Buffers.PooledBufferWriter"/>), then emit the section with
/// <see cref="MqttPropertySection.Write"/>.
/// </summary>
public ref struct MqttPropertiesWriter
{
    private MqttBufferWriter _writer;

    /// <summary>Creates a writer over the property body buffer (the bytes that follow the length prefix).</summary>
    public MqttPropertiesWriter(IBufferWriter<byte> body)
    {
        _writer = new MqttBufferWriter(body);
    }

    /// <summary>Writes a single-byte property.</summary>
    public void WriteByte(MqttPropertyId id, byte value)
    {
        WriteId(id);
        _writer.WriteByte(value);
    }

    /// <summary>Writes a two-byte integer property.</summary>
    public void WriteUInt16(MqttPropertyId id, ushort value)
    {
        WriteId(id);
        _writer.WriteUInt16(value);
    }

    /// <summary>Writes a four-byte integer property.</summary>
    public void WriteUInt32(MqttPropertyId id, uint value)
    {
        WriteId(id);
        _writer.WriteUInt32(value);
    }

    /// <summary>Writes a variable-length integer property.</summary>
    public void WriteVarInt(MqttPropertyId id, uint value)
    {
        WriteId(id);
        _writer.WriteVarInt(value);
    }

    /// <summary>Writes a UTF-8 string property.</summary>
    public void WriteString(MqttPropertyId id, string value)
    {
        WriteId(id);
        _writer.WriteString(value);
    }

    /// <summary>Writes a binary property.</summary>
    public void WriteBinary(MqttPropertyId id, ReadOnlySpan<byte> value)
    {
        WriteId(id);
        _writer.WriteBinary(value);
    }

    /// <summary>Writes a UTF-8 string pair property (name then value).</summary>
    public void WriteStringPair(MqttPropertyId id, string name, string value)
    {
        WriteId(id);
        _writer.WriteString(name);
        _writer.WriteString(value);
    }

    private void WriteId(MqttPropertyId id) => _writer.WriteVarInt((byte)id);
}
