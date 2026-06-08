namespace Pulse.Mqtt.Codec;

/// <summary>The wire type of an MQTT 5 property value.</summary>
public enum MqttPropertyType
{
    /// <summary>A single byte.</summary>
    Byte,

    /// <summary>A big-endian unsigned 16-bit integer.</summary>
    TwoByteInteger,

    /// <summary>A big-endian unsigned 32-bit integer.</summary>
    FourByteInteger,

    /// <summary>A variable-length integer.</summary>
    VariableByteInteger,

    /// <summary>A length-prefixed UTF-8 string.</summary>
    Utf8String,

    /// <summary>A length-prefixed UTF-8 string pair (name and value).</summary>
    Utf8StringPair,

    /// <summary>Length-prefixed binary data.</summary>
    BinaryData,
}
