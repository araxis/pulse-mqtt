using System.Linq;
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
    public async Task Drop_oldest_never_admits_more_than_capacity_under_concurrent_removal()
    {
        const int capacity = 2;
        var store = NewStore(capacity, OverflowPolicy.DropOldest);

        // Prime to full so enqueues take the evict path and removals free real slots.
        for (var i = 0; i < capacity; i++)
        {
            await store.EnqueueAsync(Packet("seed"), CancellationToken.None);
        }

        var overflowObserved = 0;
        using var run = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        var enqueuers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!run.IsCancellationRequested)
            {
                await store.EnqueueAsync(Packet("x"), CancellationToken.None);
                var count = store.Count;
                if (count > capacity)
                {
                    Interlocked.Exchange(ref overflowObserved, count);
                }
            }
        })).ToArray();

        var removers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!run.IsCancellationRequested)
            {
                await store.RemoveHeadAsync(CancellationToken.None);
            }
        })).ToArray();

        await Task.WhenAll(enqueuers.Concat(removers));

        // The capacity bound must hold at all times. Before the fix, the DropOldest evict path decided
        // "full" on a Wait(0) taken outside the lock, so a removal could free a slot between that check
        // and the enqueue; the enqueue then added an entry without consuming the freed permit — leaking
        // a permit so the queue admits more than Capacity entries and grows unbounded.
        Volatile.Read(ref overflowObserved).ShouldBe(0);
        store.Count.ShouldBeLessThanOrEqualTo(capacity);
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
