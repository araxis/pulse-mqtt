namespace Pulse.Mqtt.Protocol;

/// <summary>
/// MQTT 5.0 reason codes carried by CONNACK, DISCONNECT, and the acknowledgement packets.
/// Values match the single-byte codes defined by the MQTT 5.0 specification.
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>Success / normal disconnection / granted QoS 0 (0x00).</summary>
    Success = 0x00,

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

    /// <summary>The packet exceeded the negotiated maximum packet size (0x95).</summary>
    PacketTooLarge = 0x95,

    /// <summary>An implementation or administrative imposed quota was exceeded (0x97).</summary>
    QuotaExceeded = 0x97,
}
