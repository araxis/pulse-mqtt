namespace Pulse.Mqtt.Protocol;

/// <summary>The MQTT protocol level negotiated for a connection.</summary>
public enum MqttProtocolVersion
{
    /// <summary>MQTT 3.1.1 (protocol level 4).</summary>
    V311 = 4,

    /// <summary>MQTT 5.0 (protocol level 5).</summary>
    V500 = 5,
}
