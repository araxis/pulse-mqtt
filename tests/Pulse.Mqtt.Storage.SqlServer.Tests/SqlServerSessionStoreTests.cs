using System.Text;
using Pulse.Mqtt;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;

namespace Pulse.Mqtt.Storage.SqlServer.Tests;

public sealed class SqlServerSessionStoreTests
{
    [SqlServerFact]
    public async Task Subscriptions_round_trip_across_a_restart()
    {
        await using var database = SqlServerTestDatabase.Create();
        var filters = new[]
        {
            new MqttTopicFilter("sensors/+/temp") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce, NoLocal = true },
            new MqttTopicFilter("alerts/#") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce, RetainAsPublished = true, RetainHandling = MqttRetainHandling.DoNotSendAtSubscribe },
        };

        await using (var store = new SqlServerSessionStore(database.ConnectionString, database.Options))
        {
            await store.SaveSubscriptionsAsync(filters, CancellationToken.None);
        }

        await using var reopened = new SqlServerSessionStore(database.ConnectionString, database.Options);
        var loaded = await reopened.LoadSubscriptionsAsync(CancellationToken.None);

        loaded.Count.ShouldBe(2);
        loaded.ShouldContain(f => f.Topic == "sensors/+/temp" && f.MaximumQualityOfService == MqttQualityOfService.AtLeastOnce && f.NoLocal);
        loaded.ShouldContain(f => f.Topic == "alerts/#" && f.MaximumQualityOfService == MqttQualityOfService.ExactlyOnce && f.RetainAsPublished && f.RetainHandling == MqttRetainHandling.DoNotSendAtSubscribe);
    }

    [SqlServerFact]
    public async Task Upsert_replaces_by_topic_and_remove_deletes()
    {
        await using var database = SqlServerTestDatabase.Create();
        await using var store = new SqlServerSessionStore(database.ConnectionString, database.Options);

        await store.UpsertSubscriptionsAsync([new MqttTopicFilter("a") { MaximumQualityOfService = MqttQualityOfService.AtMostOnce }], CancellationToken.None);
        await store.UpsertSubscriptionsAsync([new MqttTopicFilter("b") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }], CancellationToken.None);
        await store.UpsertSubscriptionsAsync([new MqttTopicFilter("a") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }], CancellationToken.None);

        var afterUpsert = await store.LoadSubscriptionsAsync(CancellationToken.None);
        afterUpsert.Count.ShouldBe(2);
        afterUpsert.Single(f => f.Topic == "a").MaximumQualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);

        await store.RemoveSubscriptionsAsync(["a"], CancellationToken.None);
        var afterRemove = await store.LoadSubscriptionsAsync(CancellationToken.None);
        afterRemove.ShouldHaveSingleItem().Topic.ShouldBe("b");
    }

    [SqlServerFact]
    public async Task In_flight_state_round_trips_across_a_restart_in_order()
    {
        await using var database = SqlServerTestDatabase.Create();
        var state = new MqttInFlightState(
            [
                new MqttInFlightPublish(Publish("orders/1", "first", 1, MqttQualityOfService.AtLeastOnce), MqttInFlightStage.AwaitingPubAck),
                new MqttInFlightPublish(Publish("orders/2", "second", 2, MqttQualityOfService.ExactlyOnce), MqttInFlightStage.AwaitingPubComp),
            ],
            [7, 9]);

        await using (var store = new SqlServerSessionStore(database.ConnectionString, database.Options))
        {
            await store.SaveInFlightAsync(state, CancellationToken.None);
        }

        await using var reopened = new SqlServerSessionStore(database.ConnectionString, database.Options);
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

    [SqlServerFact]
    public async Task Clear_removes_subscriptions_and_in_flight_state()
    {
        await using var database = SqlServerTestDatabase.Create();
        await using var store = new SqlServerSessionStore(database.ConnectionString, database.Options);

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
