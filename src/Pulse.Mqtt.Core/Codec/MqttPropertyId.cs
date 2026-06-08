namespace Pulse.Mqtt.Codec;

/// <summary>MQTT 5 property identifiers, as defined by the specification (section 2.2.2.2).</summary>
public enum MqttPropertyId : byte
{
    /// <summary>Payload Format Indicator (byte).</summary>
    PayloadFormatIndicator = 0x01,

    /// <summary>Message Expiry Interval (four-byte integer).</summary>
    MessageExpiryInterval = 0x02,

    /// <summary>Content Type (UTF-8 string).</summary>
    ContentType = 0x03,

    /// <summary>Response Topic (UTF-8 string).</summary>
    ResponseTopic = 0x08,

    /// <summary>Correlation Data (binary).</summary>
    CorrelationData = 0x09,

    /// <summary>Subscription Identifier (variable-length integer).</summary>
    SubscriptionIdentifier = 0x0B,

    /// <summary>Session Expiry Interval (four-byte integer).</summary>
    SessionExpiryInterval = 0x11,

    /// <summary>Assigned Client Identifier (UTF-8 string).</summary>
    AssignedClientIdentifier = 0x12,

    /// <summary>Server Keep Alive (two-byte integer).</summary>
    ServerKeepAlive = 0x13,

    /// <summary>Authentication Method (UTF-8 string).</summary>
    AuthenticationMethod = 0x15,

    /// <summary>Authentication Data (binary).</summary>
    AuthenticationData = 0x16,

    /// <summary>Request Problem Information (byte).</summary>
    RequestProblemInformation = 0x17,

    /// <summary>Will Delay Interval (four-byte integer).</summary>
    WillDelayInterval = 0x18,

    /// <summary>Request Response Information (byte).</summary>
    RequestResponseInformation = 0x19,

    /// <summary>Response Information (UTF-8 string).</summary>
    ResponseInformation = 0x1A,

    /// <summary>Server Reference (UTF-8 string).</summary>
    ServerReference = 0x1C,

    /// <summary>Reason String (UTF-8 string).</summary>
    ReasonString = 0x1F,

    /// <summary>Receive Maximum (two-byte integer).</summary>
    ReceiveMaximum = 0x21,

    /// <summary>Topic Alias Maximum (two-byte integer).</summary>
    TopicAliasMaximum = 0x22,

    /// <summary>Topic Alias (two-byte integer).</summary>
    TopicAlias = 0x23,

    /// <summary>Maximum QoS (byte).</summary>
    MaximumQoS = 0x24,

    /// <summary>Retain Available (byte).</summary>
    RetainAvailable = 0x25,

    /// <summary>User Property (UTF-8 string pair).</summary>
    UserProperty = 0x26,

    /// <summary>Maximum Packet Size (four-byte integer).</summary>
    MaximumPacketSize = 0x27,

    /// <summary>Wildcard Subscription Available (byte).</summary>
    WildcardSubscriptionAvailable = 0x28,

    /// <summary>Subscription Identifier Available (byte).</summary>
    SubscriptionIdentifierAvailable = 0x29,

    /// <summary>Shared Subscription Available (byte).</summary>
    SharedSubscriptionAvailable = 0x2A,
}
