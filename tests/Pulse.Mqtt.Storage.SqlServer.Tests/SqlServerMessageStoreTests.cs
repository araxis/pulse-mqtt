using System.Text;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Storage.SqlServer.Tests;

public sealed class SqlServerMessageStoreTests
{
    private static readonly OfflineQueueOptions Unbounded = new() { Capacity = 100 };

    [SqlServerFact]
    public async Task Queue_is_fifo_across_peek_and_remove()
    {
        await using var database = SqlServerTestDatabase.Create();
        await using var store = new SqlServerMessageStore(database.ConnectionString, Unbounded, database.Options);

        await store.EnqueueAsync(Publish("a"), CancellationToken.None);
        await store.EnqueueAsync(Publish("b"), CancellationToken.None);
        store.Count.ShouldBe(2);

        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("a");
        await store.RemoveHeadAsync(CancellationToken.None);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("b");
        store.Count.ShouldBe(1);
    }

    [SqlServerFact]
    public async Task A_queued_publish_survives_a_restart_with_its_payload_intact()
    {
        await using var database = SqlServerTestDatabase.Create();
        await using (var store = new SqlServerMessageStore(database.ConnectionString, Unbounded, database.Options))
        {
            await store.EnqueueAsync(Publish("orders/1", "hello", MqttQualityOfService.AtLeastOnce), CancellationToken.None);
        }

        await using var reopened = new SqlServerMessageStore(database.ConnectionString, Unbounded, database.Options);
        reopened.Count.ShouldBe(1);
        var head = await reopened.PeekAsync(CancellationToken.None);
        head!.Topic.ShouldBe("orders/1");
        head.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        Encoding.UTF8.GetString(head.Payload.Span).ShouldBe("hello");
    }

    [SqlServerFact]
    public async Task A_qos_publish_queued_before_it_is_sent_round_trips_without_an_identifier()
    {
        await using var database = SqlServerTestDatabase.Create();
        var queued = new MqttPublishPacket
        {
            Topic = "orders/1",
            Payload = Encoding.UTF8.GetBytes("hello"),
            QualityOfService = MqttQualityOfService.ExactlyOnce,
            PacketIdentifier = null,
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        await using (var store = new SqlServerMessageStore(database.ConnectionString, Unbounded, database.Options))
        {
            await store.EnqueueAsync(queued, CancellationToken.None);
            store.Count.ShouldBe(1);
        }

        await using var reopened = new SqlServerMessageStore(database.ConnectionString, Unbounded, database.Options);
        var head = await reopened.PeekAsync(CancellationToken.None);
        head.ShouldNotBeNull();
        head.Topic.ShouldBe("orders/1");
        head.QualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);
        head.PacketIdentifier.ShouldBeNull();
        Encoding.UTF8.GetString(head.Payload.Span).ShouldBe("hello");
    }

    [SqlServerFact]
    public async Task Removing_a_flushed_entry_evicted_mid_flush_leaves_the_unsent_message()
    {
        await using var database = SqlServerTestDatabase.Create();
        await using var store = new SqlServerMessageStore(
            database.ConnectionString,
            new OfflineQueueOptions { Capacity = 2, Overflow = OverflowPolicy.DropOldest },
            database.Options);

        await store.EnqueueAsync(new MqttPublishPacket { Topic = "a" }, CancellationToken.None);
        await store.EnqueueAsync(new MqttPublishPacket { Topic = "b" }, CancellationToken.None);

        var peeked = await store.PeekQueuedAsync(CancellationToken.None);
        peeked!.Packet.Topic.ShouldBe("a");

        await store.EnqueueAsync(new MqttPublishPacket { Topic = "c" }, CancellationToken.None);
        await store.RemoveAsync(peeked, CancellationToken.None);

        store.Count.ShouldBe(2);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("b");
    }

    [SqlServerFact]
    public async Task Overflow_policies_match_the_queue_contract()
    {
        await using var dropNewest = SqlServerTestDatabase.Create();
        await using (var store = new SqlServerMessageStore(
            dropNewest.ConnectionString,
            new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.DropNewest },
            dropNewest.Options))
        {
            await store.EnqueueAsync(Publish("kept"), CancellationToken.None);
            await store.EnqueueAsync(Publish("dropped"), CancellationToken.None);

            store.Count.ShouldBe(1);
            store.DroppedCount.ShouldBe(1);
            (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("kept");
        }

        await using var dropOldest = SqlServerTestDatabase.Create();
        await using (var store = new SqlServerMessageStore(
            dropOldest.ConnectionString,
            new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.DropOldest },
            dropOldest.Options))
        {
            await store.EnqueueAsync(Publish("old"), CancellationToken.None);
            await store.EnqueueAsync(Publish("new"), CancellationToken.None);

            store.Count.ShouldBe(1);
            store.DroppedCount.ShouldBe(1);
            (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("new");
        }

        await using var reject = SqlServerTestDatabase.Create();
        await using (var store = new SqlServerMessageStore(
            reject.ConnectionString,
            new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.Reject },
            reject.Options))
        {
            await store.EnqueueAsync(Publish("first"), CancellationToken.None);
            await Should.ThrowAsync<OfflineQueueFullException>(() => store.EnqueueAsync(Publish("second"), CancellationToken.None).AsTask());

            store.Count.ShouldBe(1);
            store.DroppedCount.ShouldBe(1);
        }
    }

    [SqlServerFact]
    public async Task Enqueue_time_survives_the_round_trip()
    {
        await using var database = SqlServerTestDatabase.Create();
        await using var store = new SqlServerMessageStore(database.ConnectionString, Unbounded, database.Options);

        var stamp = new DateTimeOffset(2026, 7, 3, 12, 0, 0, TimeSpan.Zero);
        await store.EnqueueAsync(Publish("stamped"), stamp, CancellationToken.None);

        var entry = (await store.PeekQueuedAsync(CancellationToken.None))!;
        entry.Packet.Topic.ShouldBe("stamped");
        entry.EnqueuedAt.ShouldBe(stamp);
    }

    [SqlServerFact]
    public async Task Table_prefix_isolates_logical_clients()
    {
        await using var first = SqlServerTestDatabase.Create();
        await using var second = SqlServerTestDatabase.Create();

        await using (var firstStore = new SqlServerMessageStore(first.ConnectionString, Unbounded, first.Options))
        await using (var secondStore = new SqlServerMessageStore(second.ConnectionString, Unbounded, second.Options))
        {
            await firstStore.EnqueueAsync(Publish("first"), CancellationToken.None);
            await secondStore.EnqueueAsync(Publish("second"), CancellationToken.None);

            (await firstStore.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("first");
            (await secondStore.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("second");
        }
    }

    [Fact]
    public void Invalid_storage_identifiers_are_rejected_before_opening_a_connection()
    {
        Should.Throw<ArgumentException>(() => new SqlServerSessionStore(
            "Server=unused;",
            new SqlServerStorageOptions { SchemaName = "" }));

        Should.Throw<ArgumentException>(() => new SqlServerMessageStore(
            "Server=unused;",
            new OfflineQueueOptions { Capacity = 1 },
            new SqlServerStorageOptions { TablePrefix = new string('x', 120) }));
    }

    private static MqttPublishPacket Publish(string topic, string payload = "x", MqttQualityOfService qos = MqttQualityOfService.AtLeastOnce) => new()
    {
        Topic = topic,
        Payload = Encoding.UTF8.GetBytes(payload),
        QualityOfService = qos,
        PacketIdentifier = qos == MqttQualityOfService.AtMostOnce ? null : (ushort)1,
        ProtocolVersion = MqttProtocolVersion.V500,
    };
}
