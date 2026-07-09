using Microsoft.Data.SqlClient;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Storage.SqlServer;

/// <summary>
/// A durable <see cref="IMessageStore"/> over SQL Server. The offline outbound queue survives
/// restarts while preserving the same peek/remove at-least-once contract as the in-memory store.
/// </summary>
public sealed class SqlServerMessageStore : IMessageStore, IAsyncDisposable, IDisposable
{
    private readonly OfflineQueueOptions _options;
    private readonly SqlServerStore _store;
    private readonly SemaphoreSlim? _space;
    private int _count;
    private long _dropped;

    /// <summary>Opens the message store using default SQL Server table naming options.</summary>
    public SqlServerMessageStore(string connectionString, OfflineQueueOptions options)
        : this(connectionString, options, null)
    {
    }

    /// <summary>Opens the message store using <paramref name="storageOptions"/>.</summary>
    public SqlServerMessageStore(string connectionString, OfflineQueueOptions options, SqlServerStorageOptions? storageOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Capacity, 1);
        _options = options;

        var tables = SqlServerTables.Create(storageOptions);
        _store = new SqlServerStore(connectionString, storageOptions, BuildSchema(tables));

        try
        {
            var trimmed = TrimToCapacity(options.Capacity);
            if (trimmed > 0)
            {
                _dropped = trimmed;
            }

            _count = (int)_store.ExecuteScalarLong($"SELECT COUNT_BIG(*) FROM {_store.Tables.QualifiedQueue};");
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

    /// <inheritdoc />
    public async ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) =>
        (await PeekQueuedAsync(cancellationToken).ConfigureAwait(false))?.Packet;

    /// <inheritdoc />
    public ValueTask<MqttQueuedPublish?> PeekQueuedAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(async (connection, token) =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT TOP (1) Version, Packet, EnqueuedAt, Seq FROM {_store.Tables.QualifiedQueue} ORDER BY Seq;";
            using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            if (!await reader.ReadAsync(token).ConfigureAwait(false))
            {
                return (MqttQueuedPublish?)null;
            }

            var version = (MqttProtocolVersion)reader.GetInt32(0);
            var blob = await reader.GetFieldValueAsync<byte[]>(1, token).ConfigureAwait(false);
            DateTimeOffset? enqueuedAt = await reader.IsDBNullAsync(2, token).ConfigureAwait(false)
                ? null
                : DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(2));
            return new MqttQueuedPublish(PacketBlob.Decode(blob, version), enqueuedAt) { Sequence = reader.GetInt64(3) };
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(async (connection, token) =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {_store.Tables.QualifiedQueue} WHERE Seq = (SELECT TOP (1) Seq FROM {_store.Tables.QualifiedQueue} ORDER BY Seq);";
            if (await command.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0)
            {
                _count--;
                _space?.Release();
            }
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask RemoveAsync(MqttQueuedPublish entry, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return _store.RunAsync(async (connection, token) =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {_store.Tables.QualifiedQueue} WHERE Seq = @seq;";
            AddParameter(command, "@seq", entry.Sequence);
            if (await command.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0)
            {
                _count--;
                _space?.Release();
            }
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(async (connection, token) =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {_store.Tables.QualifiedQueue};";
            var removed = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
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

    private async ValueTask EnqueueCoreAsync(MqttPublishPacket packet, DateTimeOffset? enqueuedAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(packet);

        switch (_options.Overflow)
        {
            case OverflowPolicy.Block:
                await EnqueueBlockingAsync(packet, enqueuedAt, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropNewest:
                await _store.RunAsync(async (connection, token) =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return;
                    }

                    await InsertAsync(connection, transaction: null, packet, enqueuedAt, token).ConfigureAwait(false);
                    _count++;
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.DropOldest:
                await _store.RunAsync(async (connection, token) =>
                {
                    if (_count >= _options.Capacity)
                    {
                        using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
                        await DeleteHeadAsync(connection, transaction, token).ConfigureAwait(false);
                        await InsertAsync(connection, transaction, packet, enqueuedAt, token).ConfigureAwait(false);
                        await transaction.CommitAsync(token).ConfigureAwait(false);
                        Interlocked.Increment(ref _dropped);
                    }
                    else
                    {
                        await InsertAsync(connection, transaction: null, packet, enqueuedAt, token).ConfigureAwait(false);
                        _count++;
                    }
                }, cancellationToken).ConfigureAwait(false);
                return;

            case OverflowPolicy.Reject:
                var rejected = await _store.RunAsync(async (connection, token) =>
                {
                    if (_count >= _options.Capacity)
                    {
                        Interlocked.Increment(ref _dropped);
                        return true;
                    }

                    await InsertAsync(connection, transaction: null, packet, enqueuedAt, token).ConfigureAwait(false);
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

    private async ValueTask EnqueueBlockingAsync(MqttPublishPacket packet, DateTimeOffset? enqueuedAt, CancellationToken cancellationToken)
    {
        if (!_space!.Wait(0, cancellationToken))
        {
            using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_options.PublishWaitTimeout is { } timeout)
            {
                if (!await _space.WaitAsync(timeout, scoped.Token).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref _dropped);
                    throw new OfflineQueueFullException(_options.Capacity);
                }
            }
            else
            {
                await _space.WaitAsync(scoped.Token).ConfigureAwait(false);
            }
        }

        try
        {
            await _store.RunAsync(async (connection, token) =>
            {
                await InsertAsync(connection, transaction: null, packet, enqueuedAt, token).ConfigureAwait(false);
                _count++;
            }, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _space.Release();
            throw;
        }
    }

    private static string BuildSchema(SqlServerTables tables)
    {
        var schema = SqlServerTables.EscapeLiteral(tables.SchemaName);
        var queue = SqlServerTables.EscapeLiteral(tables.Queue);
        return
            $"""
            IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'{schema}' AND t.name = N'{queue}')
            BEGIN
                CREATE TABLE {tables.QualifiedQueue} (Seq bigint IDENTITY(1,1) NOT NULL PRIMARY KEY, Version int NOT NULL, Packet varbinary(max) NOT NULL, EnqueuedAt bigint NULL);
            END;
            """;
    }

    private int TrimToCapacity(int capacity)
    {
        var excess = (int)Math.Max(0, _store.ExecuteScalarLong($"SELECT COUNT_BIG(*) FROM {_store.Tables.QualifiedQueue};") - capacity);
        if (excess == 0)
        {
            return 0;
        }

        return _store.ExecuteNonQuery(
            $"""
            DELETE FROM {_store.Tables.QualifiedQueue}
            WHERE Seq IN (SELECT TOP ({excess}) Seq FROM {_store.Tables.QualifiedQueue} ORDER BY Seq);
            """);
    }

    private async ValueTask InsertAsync(SqlConnection connection, SqlTransaction? transaction, MqttPublishPacket packet, DateTimeOffset? enqueuedAt, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"INSERT INTO {_store.Tables.QualifiedQueue} (Version, Packet, EnqueuedAt) VALUES (@version, @packet, @enqueuedAt);";
        AddParameter(command, "@version", (int)packet.ProtocolVersion);
        AddParameter(command, "@packet", PacketBlob.Encode(packet));
        AddParameter(command, "@enqueuedAt", enqueuedAt is { } at ? at.ToUnixTimeMilliseconds() : DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask DeleteHeadAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"DELETE FROM {_store.Tables.QualifiedQueue} WHERE Seq = (SELECT TOP (1) Seq FROM {_store.Tables.QualifiedQueue} ORDER BY Seq);";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddParameter(SqlCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
