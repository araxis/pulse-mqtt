using Pulse.Mqtt.IntegrationTests.Brokers;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

/// <summary>
/// The bounded chaos invariant (see <see cref="ChaosScenario"/>) over plain TCP against Mosquitto —
/// part of the fast lane, so every push exercises reconnect-under-load.
/// </summary>
[Collection("mosquitto")]
public sealed class ChaosIntegrationTests(MosquittoFixture broker)
{
    [Fact]
    public Task Random_disconnects_under_load_lose_no_qos1_messages_with_a_persistent_session() =>
        ChaosScenario.RandomDisconnectsLoseNoQos1MessagesAsync(broker);
}
