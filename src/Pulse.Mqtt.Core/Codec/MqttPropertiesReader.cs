namespace Pulse.Mqtt.Codec;

/// <summary>
/// Reads the property section of an MQTT 5 packet. Construct it over the property bytes (the region
/// whose length is given by the property-length variable integer), then loop while
/// <see cref="HasRemaining"/> is true: read the identifier with <see cref="ReadId"/>, then the
/// typed value its <see cref="MqttPropertyType"/> requires.
/// </summary>
public ref struct MqttPropertiesReader
{
    private MqttBufferReader _reader;

    /// <summary>Creates a reader over the property section bytes (excluding the length prefix).</summary>
    public MqttPropertiesReader(ReadOnlySpan<byte> section)
    {
        _reader = new MqttBufferReader(section);
    }

    /// <summary>Whether unread property bytes remain in the section.</summary>
    public readonly bool HasRemaining => _reader.Remaining > 0;

    /// <summary>Reads the next property identifier.</summary>
    /// <exception cref="MqttProtocolException">The identifier is out of range or not a defined property.</exception>
    public MqttPropertyId ReadId()
    {
        var raw = _reader.ReadVarInt();
        if (raw > byte.MaxValue)
        {
            throw new MqttProtocolException($"Property identifier {raw} is out of range.");
        }

        var id = (MqttPropertyId)raw;
        _ = MqttPropertyTypes.GetValueType(id); // validates the identifier is defined
        return id;
    }

    /// <summary>Reads a single-byte property value.</summary>
    public byte ReadByte() => _reader.ReadByte();

    /// <summary>Reads a two-byte integer property value.</summary>
    public ushort ReadUInt16() => _reader.ReadUInt16();

    /// <summary>Reads a four-byte integer property value.</summary>
    public uint ReadUInt32() => _reader.ReadUInt32();

    /// <summary>Reads a variable-length integer property value.</summary>
    public uint ReadVarInt() => _reader.ReadVarInt();

    /// <summary>Reads a UTF-8 string property value.</summary>
    public string ReadString() => _reader.ReadString();

    /// <summary>Reads a binary property value as a slice of the section (no copy).</summary>
    public ReadOnlySpan<byte> ReadBinary() => _reader.ReadBinary();

    /// <summary>Reads a UTF-8 string pair property value (name then value).</summary>
    public (string Name, string Value) ReadStringPair() => (_reader.ReadString(), _reader.ReadString());
}
