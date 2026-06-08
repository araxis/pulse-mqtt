namespace Pulse.Mqtt.Codec;

/// <summary>The MQTT control packet type, carried in the high nibble of the fixed header's first byte.</summary>
public enum MqttPacketType : byte
{
    /// <summary>Client request to connect to the server.</summary>
    Connect = 1,

    /// <summary>Connect acknowledgement.</summary>
    ConnAck = 2,

    /// <summary>Publish message.</summary>
    Publish = 3,

    /// <summary>Publish acknowledgement (QoS 1).</summary>
    PubAck = 4,

    /// <summary>Publish received (QoS 2, part 1).</summary>
    PubRec = 5,

    /// <summary>Publish release (QoS 2, part 2).</summary>
    PubRel = 6,

    /// <summary>Publish complete (QoS 2, part 3).</summary>
    PubComp = 7,

    /// <summary>Client subscribe request.</summary>
    Subscribe = 8,

    /// <summary>Subscribe acknowledgement.</summary>
    SubAck = 9,

    /// <summary>Client unsubscribe request.</summary>
    Unsubscribe = 10,

    /// <summary>Unsubscribe acknowledgement.</summary>
    UnsubAck = 11,

    /// <summary>Ping request.</summary>
    PingReq = 12,

    /// <summary>Ping response.</summary>
    PingResp = 13,

    /// <summary>Disconnect notification.</summary>
    Disconnect = 14,

    /// <summary>Authentication exchange (MQTT 5.0 only).</summary>
    Auth = 15,
}
