using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class ConnectionStateChangedTests
{
    [Fact]
    public void Original_constructor_and_deconstruction_shape_still_work()
    {
        var change = new ConnectionStateChanged(
            ConnectionState.Connected,
            ConnectionState.Reconnecting,
            3,
            MqttReasonCode.ServerShuttingDown);

        var (previous, current, attempt, reason) = change;

        previous.ShouldBe(ConnectionState.Connected);
        current.ShouldBe(ConnectionState.Reconnecting);
        attempt.ShouldBe(3);
        reason.ShouldBe(MqttReasonCode.ServerShuttingDown);
    }

    [Fact]
    public void Diagnostic_details_are_additive_init_only_properties()
    {
        var error = new InvalidOperationException("boom");
        var change = new ConnectionStateChanged(
            ConnectionState.Connected,
            ConnectionState.Faulted,
            4,
            MqttReasonCode.UseAnotherServer)
        {
            ReasonString = "rebalancing",
            ServerReference = "backup.example:1883",
            Error = error,
        };

        change.ReasonString.ShouldBe("rebalancing");
        change.ServerReference.ShouldBe("backup.example:1883");
        change.Error.ShouldBeSameAs(error);
    }
}
