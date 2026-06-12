using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt;

/// <summary>
/// Raised when the broker closes the connection with a DISCONNECT packet. Carries the broker's
/// stated reason so callers (and the reconnect decision) can distinguish a polite shutdown from
/// a ban or a redirect.
/// </summary>
public sealed class MqttServerDisconnectedException : MqttException
{
    /// <summary>Creates the exception from the broker's DISCONNECT details.</summary>
    public MqttServerDisconnectedException(MqttReasonCode reasonCode, string? reasonString = null, string? serverReference = null)
        : base(BuildMessage(reasonCode, reasonString))
    {
        ReasonCode = reasonCode;
        ReasonString = reasonString;
        ServerReference = serverReference;
    }

    /// <summary>The broker's disconnect reason code.</summary>
    public MqttReasonCode ReasonCode { get; }

    /// <summary>The broker's human-readable reason, when it sent one.</summary>
    public string? ReasonString { get; }

    /// <summary>An alternate server the broker referred the client to, when it sent one.</summary>
    public string? ServerReference { get; }

    private static string BuildMessage(MqttReasonCode reasonCode, string? reasonString) =>
        reasonString is null
            ? $"The broker closed the connection: {reasonCode}."
            : $"The broker closed the connection: {reasonCode} ({reasonString}).";
}
