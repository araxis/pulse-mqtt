using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class InMemoryMessageStoreTests
{
    private static MqttPublishPacket Packet(string topic) => new() { Topic = topic };

    private static InMemoryMessageStore NewStore(int capacity, OverflowPolicy overflow, TimeSpan? waitTimeout = null) =>
        new(new OfflineQueueOptions { Capacity = capacity, Overflow = overflow, PublishWaitTimeout = waitTimeout });

    [Fact]
    public async Task Queue_is_first_in_first_out()
    {
        var store = NewStore(4, OverflowPolicy.Reject);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);
        await store.EnqueueAsync(Packet("b"), CancellationToken.None);

        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("a");
        await store.RemoveHeadAsync(CancellationToken.None);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("b");
        store.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Drop_oldest_evicts_the_head_and_counts()
    {
        var store = NewStore(2, OverflowPolicy.DropOldest);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);
        await store.EnqueueAsync(Packet("b"), CancellationToken.None);

        await store.EnqueueAsync(Packet("c"), CancellationToken.None);

        store.Count.ShouldBe(2);
        store.DroppedCount.ShouldBe(1);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("b");
    }

    [Fact]
    public async Task Removing_a_flushed_entry_evicted_mid_flush_leaves_the_unsent_message()
    {
        // Models the flush racing a DropOldest eviction: peek the head, a concurrent enqueue evicts
        // it (the flush is mid-send), then the flush removes what it peeked. RemoveAsync must remove
        // only that entry, not "the current head" — which is now a different, unsent message.
        var store = NewStore(2, OverflowPolicy.DropOldest);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);
        await store.EnqueueAsync(Packet("b"), CancellationToken.None); // queue full: [a, b]

        var peeked = await store.PeekQueuedAsync(CancellationToken.None);
        peeked!.Packet.Topic.ShouldBe("a");

        await store.EnqueueAsync(Packet("c"), CancellationToken.None); // DropOldest evicts a: [b, c]
        await store.RemoveAsync(peeked, CancellationToken.None);       // remove 'a' (already gone) — a no-op

        store.Count.ShouldBe(2);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("b"); // 'b' preserved, still to send
    }

    [Fact]
    public async Task Drop_newest_keeps_the_queue_and_counts()
    {
        var store = NewStore(2, OverflowPolicy.DropNewest);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);
        await store.EnqueueAsync(Packet("b"), CancellationToken.None);

        await store.EnqueueAsync(Packet("c"), CancellationToken.None);

        store.Count.ShouldBe(2);
        store.DroppedCount.ShouldBe(1);
        (await store.PeekAsync(CancellationToken.None))!.Topic.ShouldBe("a");
    }

    [Fact]
    public async Task Reject_throws_when_full()
    {
        var store = NewStore(1, OverflowPolicy.Reject);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);

        await Should.ThrowAsync<OfflineQueueFullException>(
            async () => await store.EnqueueAsync(Packet("b"), CancellationToken.None));
        store.DroppedCount.ShouldBe(1);
    }

    [Fact]
    public async Task Block_waits_until_space_frees_up()
    {
        var store = NewStore(1, OverflowPolicy.Block);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var blocked = store.EnqueueAsync(Packet("b"), timeout.Token).AsTask();
        await Task.Delay(100, timeout.Token);
        blocked.IsCompleted.ShouldBeFalse();

        await store.RemoveHeadAsync(timeout.Token);
        await blocked;

        store.Count.ShouldBe(1);
        (await store.PeekAsync(timeout.Token))!.Topic.ShouldBe("b");
    }

    [Fact]
    public async Task Block_fails_after_the_wait_timeout()
    {
        var store = NewStore(1, OverflowPolicy.Block, waitTimeout: TimeSpan.FromMilliseconds(50));
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);

        await Should.ThrowAsync<OfflineQueueFullException>(
            async () => await store.EnqueueAsync(Packet("b"), CancellationToken.None));
        store.DroppedCount.ShouldBe(1);
    }

    [Fact]
    public async Task Clear_restores_full_capacity()
    {
        var store = NewStore(2, OverflowPolicy.Reject);
        await store.EnqueueAsync(Packet("a"), CancellationToken.None);
        await store.EnqueueAsync(Packet("b"), CancellationToken.None);

        await store.ClearAsync(CancellationToken.None);

        store.Count.ShouldBe(0);
        await store.EnqueueAsync(Packet("c"), CancellationToken.None);
        await store.EnqueueAsync(Packet("d"), CancellationToken.None);
        store.Count.ShouldBe(2);
    }
}
