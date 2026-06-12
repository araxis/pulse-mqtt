using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// The default retry classification: identity and authentication CONNACK reasons are final;
/// everything else — server-unavailable/busy, quota, and network errors — is retried.
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
            MqttConnectRejectedException rejected => !IsTerminalReason(rejected.ReasonCode),
            _ => true,
        };
    }

    private static bool IsTerminalReason(MqttReasonCode reasonCode) => reasonCode
        is MqttReasonCode.NotAuthorized
        or MqttReasonCode.BadUserNameOrPassword
        or MqttReasonCode.ClientIdentifierNotValid
        or MqttReasonCode.BadAuthenticationMethod
        or MqttReasonCode.Banned;
}
