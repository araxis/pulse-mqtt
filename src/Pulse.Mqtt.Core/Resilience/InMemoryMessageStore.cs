using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Resilience;

/// <summary>The default offline queue: bounded, in memory, for the lifetime of the process.</summary>
public sealed class InMemoryMessageStore : IMessageStore
{
    private readonly OfflineQueueOptions _options;
    private readonly Queue<MqttPublishPacket> _queue = new();
    private readonly SemaphoreSlim _space;
    private readonly object _gate = new();
    private long _dropped;

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
    public async ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);

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
                    _queue.Enqueue(packet);
                }

                return;

            case OverflowPolicy.DropOldest:
                if (_space.Wait(0, cancellationToken))
                {
                    lock (_gate)
                    {
                        _queue.Enqueue(packet);
                    }
                }
                else
                {
                    // Evict the head and take its slot; the semaphore count is unchanged.
                    lock (_gate)
                    {
                        if (_queue.Count > 0)
                        {
                            _queue.Dequeue();
                        }

                        _queue.Enqueue(packet);
                    }

                    Interlocked.Increment(ref _dropped);
                }

                return;

            case OverflowPolicy.DropNewest:
                if (_space.Wait(0, cancellationToken))
                {
                    lock (_gate)
                    {
                        _queue.Enqueue(packet);
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
                    _queue.Enqueue(packet);
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
        }

        _space.Release();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        int removed;
        lock (_gate)
        {
            removed = _queue.Count;
            _queue.Clear();
        }

        if (removed > 0)
        {
            _space.Release(removed);
        }

        return ValueTask.CompletedTask;
    }
}
