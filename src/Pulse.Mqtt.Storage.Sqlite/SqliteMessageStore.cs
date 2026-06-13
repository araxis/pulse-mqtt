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
public sealed class SqliteMessageStore : SqliteStore, IMessageStore
{
    private readonly OfflineQueueOptions _options;
    private readonly SemaphoreSlim? _space;
    private int _count;
    private long _dropped;

    /// <summary>Opens (creating if needed) the queue database at <paramref name="connectionString"/>.</summary>
    /// <param name="connectionString">
    /// A Microsoft.Data.Sqlite connection string, or a plain file path (e.g. <c>mqtt-queue.db</c>).
    /// </param>
    /// <param name="options">Bounds and overflow policy for the queue.</param>
    public SqliteMessageStore(string connectionString, OfflineQueueOptions options)
        : base(Normalize(connectionString))
    {
        try
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentOutOfRangeException.ThrowIfLessThan(options.Capacity, 1);
            _options = options;

            // A capacity lowered between runs can leave more rows than fit; shed the oldest so the
            // count never exceeds capacity and the admission bookkeeping stays sound.
            var trimmed = TrimToCapacity(options.Capacity);
            if (trimmed > 0)
            {
                _dropped = trimmed;
            }

            _count = (int)ExecuteScalarLong("SELECT COUNT(*) FROM Queue;");
            if (options.Overflow == OverflowPolicy.Block)
            {
                _space = new SemaphoreSlim(options.Capacity - _count, options.Capacity);
            }
        }
        catch
        {
            // The base constructor already opened the connection; don't leak it on a bad argument.
            Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    protected override void EnsureSchema() => Execute(
        "CREATE TABLE IF NOT EXISTS Queue (Seq INTEGER PRIMARY KEY AUTOINCREMENT, Version INTEGER NOT NULL, Packet BLOB NOT NULL);");

    /// <inheritdoc />
    public int Count => Volatile.Read(ref _count);

    /// <inheritdoc />
    public long DroppedCount => Interlocked.Read(ref _dropped);

    /// <inheritdoc />
    public async ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);

        switch (_options.Overflow)
        {
            case OverflowPolicy.Block:
                await EnqueueBlockingAsync(packet, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropNewest:
                await RunAsync(connection =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return;
                    }

                    Insert(connection, transaction: null, packet);
                    _count++;
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropOldest:
                await RunAsync(connection =>
                {
                    if (_count >= _options.Capacity)
                    {
                        // Evict the head and take its slot; the count is unchanged.
                        using var transaction = connection.BeginTransaction();
                        DeleteHead(connection, transaction);
                        Insert(connection, transaction, packet);
                        transaction.Commit();
                        Interlocked.Increment(ref _dropped);
                    }
                    else
                    {
                        Insert(connection, transaction: null, packet);
                        _count++;
                    }
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.Reject:
                var rejected = await RunAsync(connection =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return true;
                    }

                    Insert(connection, transaction: null, packet);
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
    public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) =>
        RunAsync(connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Version, Packet FROM Queue ORDER BY Seq LIMIT 1;";
            using var reader = command.ExecuteReader();
            if (!reader.Read())
            {
                return null;
            }

            var version = (MqttProtocolVersion)(int)reader.GetInt64(0);
            return PacketBlob.Decode(reader.GetFieldValue<byte[]>(1), version);
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) =>
        RunAsync(connection =>
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
        RunAsync(connection =>
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

    private async ValueTask EnqueueBlockingAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
    {
        if (_options.PublishWaitTimeout is { } timeout)
        {
            if (!await _space!.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
            {
                Interlocked.Increment(ref _dropped);
                throw new OfflineQueueFullException(_options.Capacity);
            }
        }
        else
        {
            await _space!.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            await RunAsync(connection =>
            {
                Insert(connection, transaction: null, packet);
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
        var excess = (int)Math.Max(0, ExecuteScalarLong("SELECT COUNT(*) FROM Queue;") - capacity);
        if (excess == 0)
        {
            return 0;
        }

        using var command = CreateCommand();
        // The excess is a computed int, not user input — safe to inline; SQLite allows LIMIT only
        // inside the subquery here, not on DELETE directly.
        command.CommandText = $"DELETE FROM Queue WHERE Seq IN (SELECT Seq FROM Queue ORDER BY Seq LIMIT {excess});";
        return command.ExecuteNonQuery();
    }

    private static void Insert(SqliteConnection connection, SqliteTransaction? transaction, MqttPublishPacket packet)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO Queue (Version, Packet) VALUES ($version, $packet);";
        var version = command.CreateParameter();
        version.ParameterName = "$version";
        version.Value = (long)(int)packet.ProtocolVersion;
        command.Parameters.Add(version);
        var blob = command.CreateParameter();
        blob.ParameterName = "$packet";
        blob.Value = PacketBlob.Encode(packet);
        command.Parameters.Add(blob);
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
