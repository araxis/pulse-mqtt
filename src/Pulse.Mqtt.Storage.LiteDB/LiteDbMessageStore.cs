using LiteDB;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Storage.LiteDB;

/// <summary>
/// A durable <see cref="IMessageStore"/> over LiteDB: the offline outbound queue survives restarts.
/// Ordering, the peek/remove-head at-least-once contract, and the overflow policy match the
/// in-memory default — a crash between flushing and removing the head re-sends rather than loses.
/// </summary>
/// <remarks>
/// The row count is the single source of truth: the fullness decision for the
/// <see cref="OverflowPolicy.DropNewest"/>, <see cref="OverflowPolicy.DropOldest"/>, and
/// <see cref="OverflowPolicy.Reject"/> policies is made under the serialization gate against the
/// live count, so it never races. The admission semaphore exists only for
/// <see cref="OverflowPolicy.Block"/>, where a publisher must wait for space.
/// </remarks>
public sealed class LiteDbMessageStore : IMessageStore, IAsyncDisposable, IDisposable
{
    private const string QueueCollection = "queue";

    private readonly OfflineQueueOptions _options;
    private readonly LiteDbStore _store;
    private readonly SemaphoreSlim? _space;
    private int _count;
    private long _dropped;

    /// <summary>Opens (creating if needed) the queue database at <paramref name="connectionString"/>.</summary>
    /// <param name="connectionString">
    /// A LiteDB connection string, or a plain file path (e.g. <c>mqtt-queue.db</c>).
    /// </param>
    /// <param name="options">Bounds and overflow policy for the queue.</param>
    public LiteDbMessageStore(string connectionString, OfflineQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Capacity, 1);
        _options = options;
        _store = new LiteDbStore(connectionString);

        try
        {
            SemaphoreSlim? space = null;
            _store.RunAsync(database =>
            {
                var queue = Queue(database);
                queue.EnsureIndex("_id");

                // A capacity lowered between runs can leave more rows than fit; shed the oldest so
                // the count never exceeds capacity and the admission bookkeeping stays sound.
                var trimmed = TrimToCapacity(queue, options.Capacity);
                if (trimmed > 0)
                {
                    _dropped = trimmed;
                }

                _count = queue.Count();
                if (options.Overflow == OverflowPolicy.Block)
                {
                    space = new SemaphoreSlim(options.Capacity - _count, options.Capacity);
                }
            }, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            _space = space;
        }
        catch
        {
            _store.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public int Count => Volatile.Read(ref _count);

    /// <inheritdoc />
    public long DroppedCount => Interlocked.Read(ref _dropped);

    /// <inheritdoc />
    public ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken) =>
        EnqueueCoreAsync(packet, enqueuedAt: null, cancellationToken);

    /// <inheritdoc />
    public ValueTask EnqueueAsync(MqttPublishPacket packet, DateTimeOffset enqueuedAt, CancellationToken cancellationToken) =>
        EnqueueCoreAsync(packet, enqueuedAt, cancellationToken);

    private async ValueTask EnqueueCoreAsync(MqttPublishPacket packet, DateTimeOffset? enqueuedAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);

        switch (_options.Overflow)
        {
            case OverflowPolicy.Block:
                await EnqueueBlockingAsync(packet, enqueuedAt, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropNewest:
                await _store.RunAsync(database =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return;
                    }

                    Insert(Queue(database), packet, enqueuedAt);
                    _count++;
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropOldest:
                await _store.RunAsync(database =>
                {
                    var queue = Queue(database);
                    if (_count >= _options.Capacity)
                    {
                        DeleteHead(queue);
                        Insert(queue, packet, enqueuedAt);
                        Interlocked.Increment(ref _dropped);
                    }
                    else
                    {
                        Insert(queue, packet, enqueuedAt);
                        _count++;
                    }
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.Reject:
                var rejected = await _store.RunAsync(database =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return true;
                    }

                    Insert(Queue(database), packet, enqueuedAt);
                    _count++;
                    return false;
                }, cancellationToken).ConfigureAwait(false);

                if (rejected)
                {
                    throw new OfflineQueueFullException(_options.Capacity);
                }

                return;

            default:
                throw new InvalidOperationException($"Unknown overflow policy {_options.Overflow}.");
        }
    }

    /// <inheritdoc />
    public async ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) =>
        (await PeekQueuedAsync(cancellationToken).ConfigureAwait(false))?.Packet;

    /// <inheritdoc />
    public ValueTask<MqttQueuedPublish?> PeekQueuedAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(database =>
        {
            var head = Head(Queue(database));
            if (head is null)
            {
                return (MqttQueuedPublish?)null;
            }

            var version = (MqttProtocolVersion)head["Version"].AsInt32;
            var packet = PacketBlob.Decode(head["Packet"].AsBinary, version);
            // Rows written before queue-time tracking have no stamp and flush without expiry accounting.
            DateTimeOffset? enqueuedAt = head.TryGetValue("EnqueuedAt", out var stamp) && stamp.IsInt64
                ? DateTimeOffset.FromUnixTimeMilliseconds(stamp.AsInt64)
                : null;
            return new MqttQueuedPublish(packet, enqueuedAt);
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(database =>
        {
            if (DeleteHead(Queue(database)))
            {
                _count--;
                _space?.Release();
            }
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(database =>
        {
            var removed = Queue(database).DeleteAll();
            if (removed > 0)
            {
                _count -= removed;
                _space?.Release(removed);
            }
        }, cancellationToken);

    /// <inheritdoc />
    public void Dispose() => _store.Dispose();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private async ValueTask EnqueueBlockingAsync(MqttPublishPacket packet, DateTimeOffset? enqueuedAt, CancellationToken cancellationToken)
    {
        if (!_space!.Wait(0, cancellationToken))
        {
            using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_options.PublishWaitTimeout is { } timeout)
            {
                if (!await _space!.WaitAsync(timeout, scoped.Token).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref _dropped);
                    throw new OfflineQueueFullException(_options.Capacity);
                }
            }
            else
            {
                await _space!.WaitAsync(scoped.Token).ConfigureAwait(false);
            }
        }

        try
        {
            await _store.RunAsync(database =>
            {
                Insert(Queue(database), packet, enqueuedAt);
                _count++;
            }, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _space!.Release();
            throw;
        }
    }

    private static ILiteCollection<BsonDocument> Queue(LiteDatabase database) =>
        database.GetCollection<BsonDocument>(QueueCollection);

    private static int TrimToCapacity(ILiteCollection<BsonDocument> queue, int capacity)
    {
        var excess = Math.Max(0, queue.Count() - capacity);
        if (excess == 0)
        {
            return 0;
        }

        var ids = queue
            .Find(Query.All("_id", Query.Ascending), limit: excess)
            .Select(document => document["_id"])
            .ToArray();

        foreach (var id in ids)
        {
            queue.Delete(id);
        }

        return ids.Length;
    }

    private static void Insert(ILiteCollection<BsonDocument> queue, MqttPublishPacket packet, DateTimeOffset? enqueuedAt)
    {
        var id = NextId(queue);
        var document = new BsonDocument
        {
            ["_id"] = id,
            ["Version"] = (int)packet.ProtocolVersion,
            ["Packet"] = PacketBlob.Encode(packet),
        };
        if (enqueuedAt is { } at)
        {
            document["EnqueuedAt"] = at.ToUnixTimeMilliseconds();
        }

        queue.Insert(document);
    }

    private static long NextId(ILiteCollection<BsonDocument> queue)
    {
        var last = queue.FindOne(Query.All("_id", Query.Descending));
        return last is null ? 1 : last["_id"].AsInt64 + 1;
    }

    private static BsonDocument? Head(ILiteCollection<BsonDocument> queue) =>
        queue.FindOne(Query.All("_id", Query.Ascending));

    private static bool DeleteHead(ILiteCollection<BsonDocument> queue)
    {
        var head = Head(queue);
        return head is not null && queue.Delete(head["_id"]);
    }
}
