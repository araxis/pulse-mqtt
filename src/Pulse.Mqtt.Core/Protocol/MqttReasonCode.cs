namespace Pulse.Mqtt.Protocol;

/// <summary>
/// MQTT 5.0 reason codes carried by CONNACK, DISCONNECT, and the acknowledgement packets.
/// Values match the single-byte codes defined by the MQTT 5.0 specification.
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>Success / normal disconnection / granted QoS 0 (0x00).</summary>
    Success = 0x00,

    /// <summary>The subscription was granted at QoS 1 (SUBACK, 0x01).</summary>
    GrantedQualityOfService1 = 0x01,

    /// <summary>The subscription was granted at QoS 2 (SUBACK, 0x02).</summary>
    GrantedQualityOfService2 = 0x02,

    /// <summary>Disconnect carrying the will message (DISCONNECT, 0x04).</summary>
    DisconnectWithWillMessage = 0x04,

    /// <summary>Continue the authentication exchange (AUTH, 0x18).</summary>
    ContinueAuthentication = 0x18,

    /// <summary>Re-authenticate using the current method (AUTH, 0x19).</summary>
    ReAuthenticate = 0x19,

    /// <summary>Unspecified error (0x80).</summary>
    UnspecifiedError = 0x80,

    /// <summary>The packet could not be parsed correctly (0x81).</summary>
    MalformedPacket = 0x81,

    /// <summary>The packet contained a protocol violation (0x82).</summary>
    ProtocolError = 0x82,

    /// <summary>The Client Identifier is valid but not allowed by the server (0x85).</summary>
    ClientIdentifierNotValid = 0x85,

    /// <summary>The server does not accept the user name or password (0x86).</summary>
    BadUserNameOrPassword = 0x86,

    /// <summary>The connection is not authorized (0x87).</summary>
    NotAuthorized = 0x87,

    /// <summary>The MQTT service is unavailable (0x88).</summary>
    ServerUnavailable = 0x88,

    /// <summary>The server is busy; try again later (0x89).</summary>
    ServerBusy = 0x89,

    /// <summary>The server is shutting down (0x8B).</summary>
    ServerShuttingDown = 0x8B,

    /// <summary>The keep-alive period elapsed without a packet (0x8D).</summary>
    KeepAliveTimeout = 0x8D,

    /// <summary>The session was taken over by another connection (0x8E).</summary>
    SessionTakenOver = 0x8E,

    /// <summary>The client has been banned by the server (0x8A).</summary>
    Banned = 0x8A,

    /// <summary>The authentication method is not supported or does not match (0x8C).</summary>
    BadAuthenticationMethod = 0x8C,

    /// <summary>The topic filter is correctly formed but not accepted (0x8F).</summary>
    TopicFilterInvalid = 0x8F,

    /// <summary>The topic name is correctly formed but not accepted (0x90).</summary>
    TopicNameInvalid = 0x90,

    /// <summary>More publishes were received than the receive maximum allows (0x93).</summary>
    ReceiveMaximumExceeded = 0x93,

    /// <summary>A topic alias is invalid or exceeds the negotiated maximum (0x94).</summary>
    TopicAliasInvalid = 0x94,

    /// <summary>The packet exceeded the negotiated maximum packet size (0x95).</summary>
    PacketTooLarge = 0x95,

    /// <summary>The message rate is too high (0x96).</summary>
    MessageRateTooHigh = 0x96,

    /// <summary>An implementation or administrative imposed quota was exceeded (0x97).</summary>
    QuotaExceeded = 0x97,

    /// <summary>The connection is closed due to an administrative action (0x98).</summary>
    AdministrativeAction = 0x98,

    /// <summary>The payload does not match the payload format indicator (0x99).</summary>
    PayloadFormatInvalid = 0x99,

    /// <summary>The server does not support retained messages (0x9A).</summary>
    RetainNotSupported = 0x9A,

    /// <summary>The requested QoS is not supported (0x9B).</summary>
    QualityOfServiceNotSupported = 0x9B,

    /// <summary>The client should temporarily use another server (0x9C).</summary>
    UseAnotherServer = 0x9C,

    /// <summary>The client should permanently use another server (0x9D).</summary>
    ServerMoved = 0x9D,

    /// <summary>The server does not support shared subscriptions (0x9E).</summary>
    SharedSubscriptionsNotSupported = 0x9E,

    /// <summary>The connection rate limit was exceeded (0x9F).</summary>
    ConnectionRateExceeded = 0x9F,

    /// <summary>The maximum connection time authorized for this connection was exceeded (0xA0).</summary>
    MaximumConnectTime = 0xA0,

    /// <summary>The server does not support subscription identifiers (0xA1).</summary>
    SubscriptionIdentifiersNotSupported = 0xA1,

    /// <summary>The server does not support wildcard subscriptions (0xA2).</summary>
    WildcardSubscriptionsNotSupported = 0xA2,
}
