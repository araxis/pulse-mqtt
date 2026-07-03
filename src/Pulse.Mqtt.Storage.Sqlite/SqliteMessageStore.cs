using Microsoft.Data.Sqlite;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Storage.Sqlite;

/// <summary>
/// A durable <see cref="IMessageStore"/> over SQLite: the offline outbound queue survives restarts.
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
public sealed class SqliteMessageStore : IMessageStore, IAsyncDisposable, IDisposable
{
    private const string Schema =
        "CREATE TABLE IF NOT EXISTS Queue (Seq INTEGER PRIMARY KEY AUTOINCREMENT, Version INTEGER NOT NULL, Packet BLOB NOT NULL, EnqueuedAt INTEGER NULL);";

    private readonly OfflineQueueOptions _options;
    private readonly SqliteStore _store;
    private readonly SemaphoreSlim? _space;
    private int _count;
    private long _dropped;

    /// <summary>Opens (creating if needed) the queue database at <paramref name="connectionString"/>.</summary>
    /// <param name="connectionString">
    /// A Microsoft.Data.Sqlite connection string, or a plain file path (e.g. <c>mqtt-queue.db</c>).
    /// </param>
    /// <param name="options">Bounds and overflow policy for the queue.</param>
    public SqliteMessageStore(string connectionString, OfflineQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Capacity, 1);
        _options = options;
        _store = new SqliteStore(SqliteStore.Normalize(connectionString), Schema);

        try
        {
            // Databases created before queue-time tracking lack the EnqueuedAt column; their
            // existing rows keep a NULL stamp and flush without expiry accounting.
            if (_store.ExecuteScalarLong("SELECT COUNT(*) FROM pragma_table_info('Queue') WHERE name = 'EnqueuedAt';") == 0)
            {
                using var migrate = _store.CreateCommand();
                migrate.CommandText = "ALTER TABLE Queue ADD COLUMN EnqueuedAt INTEGER NULL;";
                migrate.ExecuteNonQuery();
            }

            // A capacity lowered between runs can leave more rows than fit; shed the oldest so the
            // count never exceeds capacity and the admission bookkeeping stays sound.
            var trimmed = TrimToCapacity(options.Capacity);
            if (trimmed > 0)
            {
                _dropped = trimmed;
            }

            _count = (int)_store.ExecuteScalarLong("SELECT COUNT(*) FROM Queue;");
            if (options.Overflow == OverflowPolicy.Block)
            {
                _space = new SemaphoreSlim(options.Capacity - _count, options.Capacity);
            }
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
                await _store.RunAsync(connection =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return;
                    }

                    Insert(connection, transaction: null, packet, enqueuedAt);
                    _count++;
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropOldest:
                await _store.RunAsync(connection =>
                {
                    if (_count >= _options.Capacity)
                    {
                        // Evict the head and take its slot; the count is unchanged.
                        using var transaction = connection.BeginTransaction();
                        DeleteHead(connection, transaction);
                        Insert(connection, transaction, packet, enqueuedAt);
                        transaction.Commit();
                        Interlocked.Increment(ref _dropped);
                    }
                    else
                    {
                        Insert(connection, transaction: null, packet, enqueuedAt);
                        _count++;
                    }
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.Reject:
                var rejected = await _store.RunAsync(connection =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return true;
                    }

                    Insert(connection, transaction: null, packet, enqueuedAt);
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
        _store.RunAsync(connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Version, Packet, EnqueuedAt FROM Queue ORDER BY Seq LIMIT 1;";
            using var reader = command.ExecuteReader();
            if (!reader.Read())
            {
                return (MqttQueuedPublish?)null;
            }

            var version = (MqttProtocolVersion)(int)reader.GetInt64(0);
            var packet = PacketBlob.Decode(reader.GetFieldValue<byte[]>(1), version);
            DateTimeOffset? enqueuedAt = reader.IsDBNull(2)
                ? null
                : DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(2));
            return new MqttQueuedPublish(packet, enqueuedAt);
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM Queue WHERE Seq = (SELECT Seq FROM Queue ORDER BY Seq LIMIT 1);";
            if (command.ExecuteNonQuery() > 0)
            {
                _count--;
                _space?.Release();
            }
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM Queue;";
            var removed = command.ExecuteNonQuery();
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
        // SemaphoreSlim.WaitAsync(token) builds an internal linked CancellationTokenSource on the
        // supplied token for every contended wait; a long-lived caller token driving a high-volume
        // offline queue accumulates those registrations until cancelling it throws. Take a free slot
        // synchronously and confine any real wait to a per-call source disposed immediately.
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
            await _store.RunAsync(connection =>
            {
                Insert(connection, transaction: null, packet, enqueuedAt);
                _count++;
            }, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The insert never landed; return the slot so capacity is not silently lost.
            _space!.Release();
            throw;
        }
    }

    private int TrimToCapacity(int capacity)
    {
        var excess = (int)Math.Max(0, _store.ExecuteScalarLong("SELECT COUNT(*) FROM Queue;") - capacity);
        if (excess == 0)
        {
            return 0;
        }

        using var command = _store.CreateCommand();
        // The excess is a computed int, not user input — safe to inline; SQLite allows LIMIT only
        // inside the subquery here, not on DELETE directly.
        command.CommandText = $"DELETE FROM Queue WHERE Seq IN (SELECT Seq FROM Queue ORDER BY Seq LIMIT {excess});";
        return command.ExecuteNonQuery();
    }

    private static void Insert(SqliteConnection connection, SqliteTransaction? transaction, MqttPublishPacket packet, DateTimeOffset? enqueuedAt)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO Queue (Version, Packet, EnqueuedAt) VALUES ($version, $packet, $enqueuedAt);";
        var version = command.CreateParameter();
        version.ParameterName = "$version";
        version.Value = (long)(int)packet.ProtocolVersion;
        command.Parameters.Add(version);
        var blob = command.CreateParameter();
        blob.ParameterName = "$packet";
        blob.Value = PacketBlob.Encode(packet);
        command.Parameters.Add(blob);
        var stamp = command.CreateParameter();
        stamp.ParameterName = "$enqueuedAt";
        stamp.Value = enqueuedAt is { } at ? at.ToUnixTimeMilliseconds() : DBNull.Value;
        command.Parameters.Add(stamp);
        command.ExecuteNonQuery();
    }

    private static void DeleteHead(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "DELETE FROM Queue WHERE Seq = (SELECT Seq FROM Queue ORDER BY Seq LIMIT 1);";
        command.ExecuteNonQuery();
    }
}
