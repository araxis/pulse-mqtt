using Pulse.Mqtt;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class SessionLifecycleTests
{
    private sealed class RecordingRegistrar : ISubscriptionRegistrar
    {
        public List<IReadOnlyList<MqttTopicFilter>> Calls { get; } = [];

        public ValueTask<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
            IReadOnlyList<MqttTopicFilter> topicFilters,
            CancellationToken cancellationToken)
        {
            Calls.Add(topicFilters);
            IReadOnlyList<MqttReasonCode> granted = topicFilters.Select(_ => MqttReasonCode.Success).ToArray();
            return ValueTask.FromResult(granted);
        }
    }

    private sealed class UpContext(MqttConnAckPacket connAck, ISubscriptionRegistrar registrar) : IConnectionUpContext
    {
        public MqttConnAckPacket ConnAck => connAck;

        public int Attempt => 1;

        public ISubscriptionRegistrar Subscriptions => registrar;
    }

    [Fact]
    public async Task Store_replaces_and_loads_the_subscription_set()
    {
        var store = new InMemorySessionStore();

        await store.SaveSubscriptionsAsync([new MqttTopicFilter("a")], CancellationToken.None);
        await store.SaveSubscriptionsAsync([new MqttTopicFilter("b"), new MqttTopicFilter("c")], CancellationToken.None);

        var loaded = await store.LoadSubscriptionsAsync(CancellationToken.None);
        loaded.Select(f => f.Topic).ShouldBe(["b", "c"]);
    }

    [Fact]
    public async Task Store_clear_empties_the_set()
    {
        var store = new InMemorySessionStore();
        await store.SaveSubscriptionsAsync([new MqttTopicFilter("a")], CancellationToken.None);

        await store.ClearAsync(CancellationToken.None);

        (await store.LoadSubscriptionsAsync(CancellationToken.None)).ShouldBeEmpty();
    }

    [Fact]
    public async Task Lifecycle_skips_resubscribe_when_the_session_was_retained()
    {
        var store = new InMemorySessionStore();
        await store.SaveSubscriptionsAsync([new MqttTopicFilter("a")], CancellationToken.None);
        var registrar = new RecordingRegistrar();

        await new DefaultConnectionLifecycle(store).OnConnectionUpAsync(
            new UpContext(new MqttConnAckPacket { SessionPresent = true }, registrar), CancellationToken.None);

        registrar.Calls.ShouldBeEmpty();
    }

    [Fact]
    public async Task Lifecycle_skips_resubscribe_when_nothing_is_stored()
    {
        var registrar = new RecordingRegistrar();

        await new DefaultConnectionLifecycle(new InMemorySessionStore()).OnConnectionUpAsync(
            new UpContext(new MqttConnAckPacket { SessionPresent = false }, registrar), CancellationToken.None);

        registrar.Calls.ShouldBeEmpty();
    }

    [Fact]
    public async Task Lifecycle_replays_the_stored_set_when_the_session_was_lost()
    {
        var store = new InMemorySessionStore();
        await store.SaveSubscriptionsAsync(
            [new MqttTopicFilter("a/+") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }, new MqttTopicFilter("b")],
            CancellationToken.None);
        var registrar = new RecordingRegistrar();

        await new DefaultConnectionLifecycle(store).OnConnectionUpAsync(
            new UpContext(new MqttConnAckPacket { SessionPresent = false }, registrar), CancellationToken.None);

        var call = registrar.Calls.ShouldHaveSingleItem();
        call.Select(f => f.Topic).ShouldBe(["a/+", "b"]);
        call[0].MaximumQualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
    }
}
