using System.IO;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class DefaultReconnectDecisionTests
{
    private readonly DefaultReconnectDecision _decision = new();

    [Theory]
    [InlineData(MqttReasonCode.NotAuthorized, false)]
    [InlineData(MqttReasonCode.BadUserNameOrPassword, false)]
    [InlineData(MqttReasonCode.ClientIdentifierNotValid, false)]
    [InlineData(MqttReasonCode.BadAuthenticationMethod, false)]
    [InlineData(MqttReasonCode.Banned, false)]
    [InlineData(MqttReasonCode.ServerUnavailable, true)]
    [InlineData(MqttReasonCode.ServerBusy, true)]
    [InlineData(MqttReasonCode.QuotaExceeded, true)]
    public void Classifies_connack_reasons(MqttReasonCode reasonCode, bool shouldRetry)
    {
        _decision.ShouldRetry(1, new MqttConnectRejectedException(reasonCode)).ShouldBe(shouldRetry);
    }

    [Fact]
    public void Network_errors_are_transient()
    {
        _decision.ShouldRetry(1, new IOException("socket reset")).ShouldBeTrue();
    }

    [Fact]
    public void A_pre_classified_terminal_failure_is_not_retried()
    {
        _decision.ShouldRetry(1, new TerminalMqttConnectException("final")).ShouldBeFalse();
    }

    [Fact]
    public void A_pre_classified_transient_failure_is_retried()
    {
        _decision.ShouldRetry(1, new TransientMqttConnectException("temporary")).ShouldBeTrue();
    }
}
