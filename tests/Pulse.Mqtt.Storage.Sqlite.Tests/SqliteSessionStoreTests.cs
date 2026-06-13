using System.Text;
using Pulse.Mqtt;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Storage.Sqlite.Tests;

public sealed class SqliteSessionStoreTests
{
    [Fact]
    public async Task Subscriptions_round_trip_across_a_restart()
    {
        using var database = new TempDatabase();
        var filters = new[]
        {
            new MqttTopicFilter("sensors/+/temp") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce, NoLocal = true },
            new MqttTopicFilter("alerts/#") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce, RetainAsPublished = true, RetainHandling = MqttRetainHandling.DoNotSendAtSubscribe },
        };

        await using (var store = new SqliteSessionStore(database.Path))
        {
            await store.SaveSubscriptionsAsync(filters, CancellationToken.None);
        }

        await using var reopened = new SqliteSessionStore(database.Path);
        var loaded = await reopened.LoadSubscriptionsAsync(CancellationToken.None);

        loaded.Count.ShouldBe(2);
        loaded.ShouldContain(f => f.Topic == "sensors/+/temp" && f.MaximumQualityOfService == MqttQualityOfService.AtLeastOnce && f.NoLocal);
        loaded.ShouldContain(f => f.Topic == "alerts/#" && f.MaximumQualityOfService == MqttQualityOfService.ExactlyOnce && f.RetainAsPublished && f.RetainHandling == MqttRetainHandling.DoNotSendAtSubscribe);
    }

    [Fact]
    public async Task Upsert_replaces_by_topic_and_remove_deletes()
    {
        using var database = new TempDatabase();
        await using var store = new SqliteSessionStore(database.Path);

        await store.UpsertSubscriptionsAsync([new MqttTopicFilter("a") { MaximumQualityOfService = MqttQualityOfService.AtMostOnce }], CancellationToken.None);
        await store.UpsertSubscriptionsAsync([new MqttTopicFilter("b") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }], CancellationToken.None);
        // Re-upserting "a" replaces, not duplicates.
        await store.UpsertSubscriptionsAsync([new MqttTopicFilter("a") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }], CancellationToken.None);

        var afterUpsert = await store.LoadSubscriptionsAsync(CancellationToken.None);
        afterUpsert.Count.ShouldBe(2);
        afterUpsert.Single(f => f.Topic == "a").MaximumQualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);

        await store.RemoveSubscriptionsAsync(["a"], CancellationToken.None);
        var afterRemove = await store.LoadSubscriptionsAsync(CancellationToken.None);
        afterRemove.ShouldHaveSingleItem().Topic.ShouldBe("b");
    }

    [Fact]
    public async Task In_flight_state_round_trips_across_a_restart_in_order()
    {
        using var database = new TempDatabase();
        var state = new MqttInFlightState(
            [
                new MqttInFlightPublish(Publish("orders/1", "first", 1, MqttQualityOfService.AtLeastOnce), MqttInFlightStage.AwaitingPubAck),
                new MqttInFlightPublish(Publish("orders/2", "second", 2, MqttQualityOfService.ExactlyOnce), MqttInFlightStage.AwaitingPubComp),
            ],
            [7, 9]);

        await using (var store = new SqliteSessionStore(database.Path))
        {
            await store.SaveInFlightAsync(state, CancellationToken.None);
        }

        await using var reopened = new SqliteSessionStore(database.Path);
        var loaded = await reopened.LoadInFlightAsync(CancellationToken.None);

        loaded.ShouldNotBeNull();
        loaded!.Outbound.Count.ShouldBe(2);
        loaded.Outbound[0].Stage.ShouldBe(MqttInFlightStage.AwaitingPubAck);
        loaded.Outbound[0].Packet.Topic.ShouldBe("orders/1");
        loaded.Outbound[0].Packet.PacketIdentifier.ShouldBe((ushort)1);
        Encoding.UTF8.GetString(loaded.Outbound[0].Packet.Payload.Span).ShouldBe("first");
        loaded.Outbound[1].Stage.ShouldBe(MqttInFlightStage.AwaitingPubComp);
        loaded.Outbound[1].Packet.QualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);
        loaded.InboundExactlyOnce.ShouldBe([(ushort)7, (ushort)9]);
    }

    [Fact]
    public async Task Saving_in_flight_replaces_the_previous_snapshot()
    {
        using var database = new TempDatabase();
        await using var store = new SqliteSessionStore(database.Path);

        await store.SaveInFlightAsync(new MqttInFlightState([new MqttInFlightPublish(Publish("t", "x", 1, MqttQualityOfService.AtLeastOnce), MqttInFlightStage.AwaitingPubAck)], [1, 2, 3]), CancellationToken.None);
        await store.SaveInFlightAsync(new MqttInFlightState([], [4]), CancellationToken.None);

        var loaded = await store.LoadInFlightAsync(CancellationToken.None);
        loaded.ShouldNotBeNull();
        loaded!.Outbound.ShouldBeEmpty();
        loaded.InboundExactlyOnce.ShouldBe([(ushort)4]);
    }

    [Fact]
    public async Task Load_in_flight_returns_null_when_nothing_is_stored()
    {
        using var database = new TempDatabase();
        await using var store = new SqliteSessionStore(database.Path);

        (await store.LoadInFlightAsync(CancellationToken.None)).ShouldBeNull();
    }

    [Fact]
    public async Task Clear_removes_subscriptions_and_in_flight_state()
    {
        using var database = new TempDatabase();
        await using var store = new SqliteSessionStore(database.Path);

        await store.SaveSubscriptionsAsync([new MqttTopicFilter("a")], CancellationToken.None);
        await store.SaveInFlightAsync(new MqttInFlightState([], [1]), CancellationToken.None);

        await store.ClearAsync(CancellationToken.None);

        (await store.LoadSubscriptionsAsync(CancellationToken.None)).ShouldBeEmpty();
        (await store.LoadInFlightAsync(CancellationToken.None)).ShouldBeNull();
    }

    private static MqttPublishPacket Publish(string topic, string payload, ushort id, MqttQualityOfService qos) => new()
    {
        Topic = topic,
        Payload = Encoding.UTF8.GetBytes(payload),
        QualityOfService = qos,
        PacketIdentifier = id,
        ProtocolVersion = MqttProtocolVersion.V500,
    };
}
