using Pulse.Mqtt.Protocol;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests.Brokers;

// One thin class per broker so xunit starts each container only when its tests run. The shared
// scenarios live in BrokerScenarios. EMQX and HiveMQ carry the "BrokerMatrix" category so the
// per-PR build can exclude their heavier images and run them only on main.

[Collection("mosquitto")]
public sealed class MosquittoCompatibilityTests(MosquittoFixture broker) : BrokerCompatibilitySuite(broker);

[Trait("Category", "BrokerMatrix")]
[Collection("emqx")]
public sealed class EmqxCompatibilityTests(EmqxBroker broker) : BrokerCompatibilitySuite(broker);

// The same suite through the QUIC listener of the same container — proves the QUIC transport
// against a real broker, not just loopback. Requires msquic on the machine running the matrix.
[Trait("Category", "BrokerMatrix")]
[Collection("emqx")]
public sealed class EmqxQuicCompatibilityTests(EmqxBroker broker) : BrokerCompatibilitySuite(new EmqxQuicBroker(broker));

// Reconnect-under-chaos over QUIC: random connection kills under sustained QoS 1 load must lose
// nothing, exactly as over TCP. This is the resilience layer's QUIC gate, not just the codec's.
[Trait("Category", "BrokerMatrix")]
[Collection("emqx")]
public sealed class EmqxQuicChaosTests(EmqxBroker broker)
{
    [Fact]
    public Task Random_disconnects_under_load_lose_no_qos1_messages_with_a_persistent_session() =>
        ChaosScenario.RandomDisconnectsLoseNoQos1MessagesAsync(new EmqxQuicBroker(broker));
}

[Trait("Category", "BrokerMatrix")]
[Collection("hivemq")]
public sealed class HiveMqCompatibilityTests(HiveMqBroker broker) : BrokerCompatibilitySuite(broker);

/// <summary>The conformance suite each broker runs, delegating to <see cref="BrokerScenarios"/>.</summary>
public abstract class BrokerCompatibilitySuite(IMqttBroker broker)
{
    [Fact]
    public Task Handshake() => BrokerScenarios.HandshakeAsync(broker);

    [Theory]
    [InlineData(MqttQualityOfService.AtMostOnce)]
    [InlineData(MqttQualityOfService.AtLeastOnce)]
    [InlineData(MqttQualityOfService.ExactlyOnce)]
    public Task Round_trip(MqttQualityOfService qos) => BrokerScenarios.RoundTripAsync(broker, qos);

    [Fact]
    public Task Retained_message() => BrokerScenarios.RetainedMessageAsync(broker);

    [Fact]
    public Task Large_payload() => BrokerScenarios.LargePayloadAsync(broker);

    [Fact]
    public Task Shared_subscription() => BrokerScenarios.SharedSubscriptionAsync(broker);

    [Fact]
    public Task Persistent_session_resumes() => BrokerScenarios.PersistentSessionResumesAsync(broker);

    [Fact]
    public Task Receive_maximum_flow_control() => BrokerScenarios.ReceiveMaximumFlowControlAsync(broker);

    [Fact]
    public Task Topic_aliases() => BrokerScenarios.TopicAliasesAsync(broker);
}
