using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Resilience;

/// <summary>The default offline queue: bounded, in memory, for the lifetime of the process.</summary>
public sealed class InMemoryMessageStore : IMessageStore
{
    private readonly OfflineQueueOptions _options;
    private readonly Queue<MqttQueuedPublish> _queue = new();
    private readonly SemaphoreSlim _space;
    private readonly object _gate = new();
    private long _dropped;
    private long _nextSequence;

    /// <summary>Creates a store with the given bounds and overflow policy.</summary>
    public InMemoryMessageStore(OfflineQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Capacity, 1);

        _options = options;
        _space = new SemaphoreSlim(options.Capacity, options.Capacity);
    }

    /// <inheritdoc />
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _queue.Count;
            }
        }
    }

    /// <inheritdoc />
    public long DroppedCount => Interlocked.Read(ref _dropped);

    /// <inheritdoc />
    public ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken) =>
        EnqueueCoreAsync(new MqttQueuedPublish(packet, EnqueuedAt: null), cancellationToken);

    /// <inheritdoc />
    public ValueTask EnqueueAsync(MqttPublishPacket packet, DateTimeOffset enqueuedAt, CancellationToken cancellationToken) =>
        EnqueueCoreAsync(new MqttQueuedPublish(packet, enqueuedAt), cancellationToken);

    private async ValueTask EnqueueCoreAsync(MqttQueuedPublish entry, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry.Packet);

        switch (_options.Overflow)
        {
            case OverflowPolicy.Block:
                // WaitScopedAsync keeps the internal linked CancellationTokenSource that SemaphoreSlim
                // builds per contended wait off the (possibly long-lived) caller token, so a high-volume
                // offline queue cannot accumulate registrations on it.
                if (_options.PublishWaitTimeout is { } timeout)
                {
                    if (!await _space.WaitScopedAsync(timeout, cancellationToken).ConfigureAwait(false))
                    {
                        Interlocked.Increment(ref _dropped);
                        throw new OfflineQueueFullException(_options.Capacity);
                    }
                }
                else
                {
                    await _space.WaitScopedAsync(cancellationToken).ConfigureAwait(false);
                }

                lock (_gate)
                {
                    _queue.Enqueue(entry with { Sequence = ++_nextSequence });
                }

                return;

            case OverflowPolicy.DropOldest:
                cancellationToken.ThrowIfCancellationRequested();
                lock (_gate)
                {
                    // Decide evict-vs-take-slot atomically under the lock. Wait(0) is non-blocking, so
                    // it is safe to call here, and because the removers release their permit under the
                    // same lock, permit == 0 is exactly equivalent to "queue is full" at this instant.
                    // Making the check outside the lock (as before) let a concurrent drain free a slot
                    // between the check and the enqueue, so this branch enqueued without consuming the
                    // freed permit — leaking a permit and admitting more than Capacity entries.
                    if (_space.Wait(0))
                    {
                        _queue.Enqueue(entry with { Sequence = ++_nextSequence });
                    }
                    else
                    {
                        // Full: evict the head and reuse its slot, leaving both the queue length and
                        // the permit count unchanged.
                        _queue.Dequeue();
                        _queue.Enqueue(entry with { Sequence = ++_nextSequence });
                        Interlocked.Increment(ref _dropped);
                    }
                }

                return;

            case OverflowPolicy.DropNewest:
                if (_space.Wait(0, cancellationToken))
                {
                    lock (_gate)
                    {
                        _queue.Enqueue(entry with { Sequence = ++_nextSequence });
                    }
                }
                else
                {
                    Interlocked.Increment(ref _dropped);
                }

                return;

            case OverflowPolicy.Reject:
                if (!_space.Wait(0, cancellationToken))
                {
                    Interlocked.Increment(ref _dropped);
                    throw new OfflineQueueFullException(_options.Capacity);
                }

                lock (_gate)
                {
                    _queue.Enqueue(entry with { Sequence = ++_nextSequence });
                }

                return;

            default:
                throw new InvalidOperationException($"Unknown overflow policy {_options.Overflow}.");
        }
    }

    /// <inheritdoc />
    public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return ValueTask.FromResult(_queue.TryPeek(out var head) ? head.Packet : null);
        }
    }

    /// <inheritdoc />
    public ValueTask<MqttQueuedPublish?> PeekQueuedAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return ValueTask.FromResult(_queue.TryPeek(out var head) ? head : null);
        }
    }

    /// <inheritdoc />
    public ValueTask RemoveHeadAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_queue.Count == 0)
            {
                return ValueTask.CompletedTask;
            }

            _queue.Dequeue();

            // Release the permit under the same lock as the dequeue so the queue length and the
            // permit count are never transiently inconsistent — the DropOldest enqueue path relies
            // on permit == 0 meaning exactly "full" while it holds the lock.
            _space.Release();
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RemoveAsync(MqttQueuedPublish entry, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        lock (_gate)
        {
            // Only remove the flushed entry if it is still the head. A DropOldest enqueue can evict
            // the head (the entry being flushed) between the peek and here; the peeked entry is then
            // already gone and the current head is a different, unsent message that must stay. Since
            // eviction only removes from the head, a peeked entry is either still the head or gone.
            if (!_queue.TryPeek(out var head) || head.Sequence != entry.Sequence)
            {
                return ValueTask.CompletedTask;
            }

            _queue.Dequeue();

            // Release under the lock — see RemoveHeadAsync.
            _space.Release();
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var removed = _queue.Count;
            _queue.Clear();

            // Release under the lock — see RemoveHeadAsync.
            if (removed > 0)
            {
                _space.Release(removed);
            }
        }

        return ValueTask.CompletedTask;
    }
}
