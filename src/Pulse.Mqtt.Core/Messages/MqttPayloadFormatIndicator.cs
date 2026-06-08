namespace Pulse.Mqtt;

/// <summary>The MQTT 5 payload format indicator describing how a message payload is encoded.</summary>
public enum MqttPayloadFormatIndicator : byte
{
    /// <summary>Unspecified bytes (the default).</summary>
    Unspecified = 0,

    /// <summary>A UTF-8 encoded character payload.</summary>
    Utf8 = 1,
}
