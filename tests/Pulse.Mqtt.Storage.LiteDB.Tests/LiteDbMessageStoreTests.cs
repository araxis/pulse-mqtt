using System.Text;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Storage.LiteDB.Tests;

public sealed class LiteDbMessageStoreTests
{
    private static readonly OfflineQueueOptions Unbounded = new() { Capacity = 100 };

    [Fact]
    public async Task Queue_is_fifo_across_peek_and_remove()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, Unbounded);

        await store.EnqueueAsync(Publish("a"), CancellationToken.None);
        await store.EnqueueAsync(Publish("b"), CancellationToken.None);
        store.Count.ShouldBe(2);

        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("a");
        await store.RemoveHeadAsync(CancellationToken.None);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("b");
        store.Count.ShouldBe(1);
    }

    [Fact]
    public async Task A_queued_publish_survives_a_restart_with_its_payload_intact()
    {
        using var database = new TempDatabase();
        await using (var store = new LiteDbMessageStore(database.Path, Unbounded))
        {
            await store.EnqueueAsync(Publish("orders/1", "hello", MqttQualityOfService.AtLeastOnce), CancellationToken.None);
        }

        await using var reopened = new LiteDbMessageStore(database.Path, Unbounded);
        reopened.Count.ShouldBe(1);
        var head = await reopened.PeekAsync(CancellationToken.None);
        head!.Topic.ShouldBe("orders/1");
        head.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        Encoding.UTF8.GetString(head.Payload.Span).ShouldBe("hello");
    }

    [Fact]
    public async Task A_peeked_but_not_removed_publish_is_resent_after_a_crash()
    {
        using var database = new TempDatabase();

        await using (var store = new LiteDbMessageStore(database.Path, Unbounded))
        {
            await store.EnqueueAsync(Publish("orders/1"), CancellationToken.None);
            (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("orders/1");
        }

        await using var reopened = new LiteDbMessageStore(database.Path, Unbounded);
        reopened.Count.ShouldBe(1);
        (await reopened.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("orders/1");
    }

    [Fact]
    public async Task Drop_newest_keeps_the_queue_and_counts_the_drop()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.DropNewest });

        await store.EnqueueAsync(Publish("kept"), CancellationToken.None);
        await store.EnqueueAsync(Publish("dropped"), CancellationToken.None);

        store.Count.ShouldBe(1);
        store.DroppedCount.ShouldBe(1);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("kept");
    }

    [Fact]
    public async Task Drop_oldest_evicts_the_head_and_counts_the_drop()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.DropOldest });

        await store.EnqueueAsync(Publish("old"), CancellationToken.None);
        await store.EnqueueAsync(Publish("new"), CancellationToken.None);

        store.Count.ShouldBe(1);
        store.DroppedCount.ShouldBe(1);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("new");
    }

    [Fact]
    public async Task Reject_throws_when_full()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.Reject });

        await store.EnqueueAsync(Publish("first"), CancellationToken.None);
        await Should.ThrowAsync<OfflineQueueFullException>(() => store.EnqueueAsync(Publish("second"), CancellationToken.None).AsTask());

        store.Count.ShouldBe(1);
        store.DroppedCount.ShouldBe(1);
    }

    [Fact]
    public async Task Block_with_a_timeout_throws_when_space_never_frees()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions
        {
            Capacity = 1,
            Overflow = OverflowPolicy.Block,
            PublishWaitTimeout = TimeSpan.FromMilliseconds(50),
        });

        await store.EnqueueAsync(Publish("first"), CancellationToken.None);
        await Should.ThrowAsync<OfflineQueueFullException>(() => store.EnqueueAsync(Publish("second"), CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task Capacity_is_restored_from_the_existing_rows_on_open()
    {
        using var database = new TempDatabase();
        var options = new OfflineQueueOptions { Capacity = 2, Overflow = OverflowPolicy.Reject };

        await using (var store = new LiteDbMessageStore(database.Path, options))
        {
            await store.EnqueueAsync(Publish("a"), CancellationToken.None);
            await store.EnqueueAsync(Publish("b"), CancellationToken.None);
        }

        await using var reopened = new LiteDbMessageStore(database.Path, options);
        reopened.Count.ShouldBe(2);
        await Should.ThrowAsync<OfflineQueueFullException>(() => reopened.EnqueueAsync(Publish("c"), CancellationToken.None).AsTask());

        await reopened.RemoveHeadAsync(CancellationToken.None);
        await reopened.EnqueueAsync(Publish("c"), CancellationToken.None);
        reopened.Count.ShouldBe(2);
    }

    [Fact]
    public async Task Clear_empties_the_queue_and_frees_capacity()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 2, Overflow = OverflowPolicy.Reject });

        await store.EnqueueAsync(Publish("a"), CancellationToken.None);
        await store.EnqueueAsync(Publish("b"), CancellationToken.None);
        await store.ClearAsync(CancellationToken.None);

        store.Count.ShouldBe(0);
        await store.EnqueueAsync(Publish("c"), CancellationToken.None);
        store.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Reopening_with_a_smaller_capacity_trims_the_oldest_and_drains_without_error()
    {
        using var database = new TempDatabase();
        await using (var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 3, Overflow = OverflowPolicy.DropNewest }))
        {
            await store.EnqueueAsync(Publish("m0"), CancellationToken.None);
            await store.EnqueueAsync(Publish("m1"), CancellationToken.None);
            await store.EnqueueAsync(Publish("m2"), CancellationToken.None);
        }

        await using var reopened = new LiteDbMessageStore(database.Path, new OfflineQueueOptions
        {
            Capacity = 1,
            Overflow = OverflowPolicy.Block,
            PublishWaitTimeout = TimeSpan.FromMilliseconds(50),
        });

        reopened.Count.ShouldBe(1);
        reopened.DroppedCount.ShouldBe(2);
        (await reopened.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("m2");

        await Should.ThrowAsync<OfflineQueueFullException>(() => reopened.EnqueueAsync(Publish("x"), CancellationToken.None).AsTask());
        await reopened.RemoveHeadAsync(CancellationToken.None);
        await reopened.ClearAsync(CancellationToken.None);
        await reopened.EnqueueAsync(Publish("y"), CancellationToken.None);
        reopened.Count.ShouldBe(1);
    }

    [Fact]
    public async Task A_cancelled_blocking_enqueue_does_not_lose_capacity()
    {
        using var database = new TempDatabase();
        await using var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.Block });
        await store.EnqueueAsync(Publish("first"), CancellationToken.None);

        using var cancellation = new CancellationTokenSource();
        var blocked = store.EnqueueAsync(Publish("second"), cancellation.Token).AsTask();
        await Task.Delay(50);
        await cancellation.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => blocked);

        await store.RemoveHeadAsync(CancellationToken.None);
        await store.EnqueueAsync(Publish("third"), CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        store.Count.ShouldBe(1);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("third");
    }

    [Fact]
    public void A_capacity_below_one_is_rejected()
    {
        using var database = new TempDatabase();
        Should.Throw<ArgumentOutOfRangeException>(() => new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 0 }));
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
