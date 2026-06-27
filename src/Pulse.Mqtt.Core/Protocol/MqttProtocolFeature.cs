namespace Pulse.Mqtt.Protocol;

/// <summary>MQTT protocol features whose availability depends on the negotiated protocol version.</summary>
public enum MqttProtocolFeature
{
    /// <summary>MQTT 5 user properties on packets that support them.</summary>
    UserProperties = 0,

    /// <summary>MQTT 5 publish metadata such as payload format, content type, response topic, and correlation data.</summary>
    PublishProperties = 1,

    /// <summary>MQTT 5 will-message metadata such as will delay, payload format, content type, and user properties.</summary>
    WillProperties = 2,

    /// <summary>MQTT 5 CONNECT metadata such as session expiry, receive maximum, topic alias maximum, and user properties.</summary>
    ConnectProperties = 3,

    /// <summary>MQTT 5 reason strings and server references.</summary>
    ReasonMetadata = 4,

    /// <summary>MQTT 5 response-topic and correlation-data request/response metadata.</summary>
    RequestResponse = 5,

    /// <summary>W3C trace context propagated through MQTT 5 user properties.</summary>
    TraceContextUserProperties = 6,

    /// <summary>MQTT 5 subscription options such as no-local, retain-as-published, and retain handling.</summary>
    SubscriptionOptions = 7,

    /// <summary>MQTT 5 subscription identifiers.</summary>
    SubscriptionIdentifiers = 8,

    /// <summary>MQTT 5 topic aliases.</summary>
    TopicAliases = 9,

    /// <summary>MQTT 5 enhanced authentication and re-authentication.</summary>
    EnhancedAuthentication = 10,

    /// <summary>MQTT 5 session expiry interval.</summary>
    SessionExpiry = 11,

    /// <summary>MQTT 5 receive maximum flow-control negotiation.</summary>
    ReceiveMaximum = 12,

    /// <summary>MQTT 5 maximum packet size negotiation.</summary>
    MaximumPacketSize = 13,

    /// <summary>MQTT 5 server keep-alive override.</summary>
    ServerKeepAlive = 14,

    /// <summary>MQTT 5 standardized shared subscriptions.</summary>
    SharedSubscriptions = 15,
}
