using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// The default retry classification: identity and authentication CONNACK reasons are final, as
/// are broker disconnects that say "stop" — bans, takeovers, redirects. Everything else —
/// server-unavailable/busy, quota, network errors, orderly shutdowns — is retried.
/// </summary>
public sealed class DefaultReconnectDecision : IReconnectDecision
{
    /// <inheritdoc />
    public bool ShouldRetry(int attempt, Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        return error switch
        {
            TerminalMqttConnectException => false,
            TransientMqttConnectException => true,
            MqttConnectRejectedException rejected => !IsTerminalConnectReason(rejected.ReasonCode),
            MqttServerDisconnectedException disconnected => !IsTerminalDisconnectReason(disconnected.ReasonCode),
            _ => true,
        };
    }

    private static bool IsTerminalConnectReason(MqttReasonCode reasonCode) => reasonCode
        is MqttReasonCode.NotAuthorized
        or MqttReasonCode.BadUserNameOrPassword
        or MqttReasonCode.ClientIdentifierNotValid
        or MqttReasonCode.BadAuthenticationMethod
        or MqttReasonCode.Banned;

    // SessionTakenOver is terminal on purpose: another connection owns the session now, and
    // auto-reconnecting would steal it back in an endless takeover war.
    private static bool IsTerminalDisconnectReason(MqttReasonCode reasonCode) => reasonCode
        is MqttReasonCode.NotAuthorized
        or MqttReasonCode.Banned
        or MqttReasonCode.SessionTakenOver
        or MqttReasonCode.ServerMoved
        or MqttReasonCode.UseAnotherServer;
}
