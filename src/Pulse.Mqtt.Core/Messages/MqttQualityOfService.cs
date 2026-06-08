namespace Pulse.Mqtt;

/// <summary>The MQTT delivery guarantee for a published message or subscription.</summary>
public enum MqttQualityOfService : byte
{
    /// <summary>At most once delivery ("fire and forget"). QoS 0.</summary>
    AtMostOnce = 0,

    /// <summary>At least once delivery (acknowledged). QoS 1.</summary>
    AtLeastOnce = 1,

    /// <summary>Exactly once delivery (four-part handshake). QoS 2.</summary>
    ExactlyOnce = 2,
}
