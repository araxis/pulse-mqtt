using Microsoft.Data.SqlClient;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Storage.SqlServer;

/// <summary>
/// A durable <see cref="ISessionStore"/> over SQL Server. Subscriptions and in-flight QoS state are
/// persisted so a persistent-session client resumes after a process restart.
/// </summary>
public sealed class SqlServerSessionStore : ISessionStore, IAsyncDisposable, IDisposable
{
    private readonly SqlServerStore _store;

    /// <summary>Opens the session store and creates missing tables using default naming options.</summary>
    public SqlServerSessionStore(string connectionString)
        : this(connectionString, null)
    {
    }

    /// <summary>Opens the session store and creates missing tables using <paramref name="options"/>.</summary>
    public SqlServerSessionStore(string connectionString, SqlServerStorageOptions? options)
    {
        var tables = SqlServerTables.Create(options);
        _store = new SqlServerStore(connectionString, options, BuildSchema(tables));
    }

    /// <inheritdoc />
    public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        return _store.RunAsync(async (connection, token) =>
        {
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, $"DELETE FROM {_store.Tables.QualifiedSubscriptions};", token).ConfigureAwait(false);
            await UpsertSubscriptionsAsync(connection, transaction, topicFilters, token).ConfigureAwait(false);
            await transaction.CommitAsync(token).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(async (connection, token) =>
        {
            var filters = new List<MqttTopicFilter>();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT Topic, Options FROM {_store.Tables.QualifiedSubscriptions} ORDER BY Topic;";
            using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                filters.Add(MqttTopicFilter.FromSubscriptionOptions(reader.GetString(0), reader.GetByte(1)));
            }

            return (IReadOnlyList<MqttTopicFilter>)filters;
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        return _store.RunAsync(async (connection, token) =>
        {
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            await UpsertSubscriptionsAsync(connection, transaction, topicFilters, token).ConfigureAwait(false);
            await transaction.CommitAsync(token).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveSubscriptionsAsync(IReadOnlyList<string> topics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topics);
        return _store.RunAsync(async (connection, token) =>
        {
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"DELETE FROM {_store.Tables.QualifiedSubscriptions} WHERE Topic = @topic;";
            var topic = AddParameter(command, "@topic");
            foreach (var value in topics)
            {
                topic.Value = value;
                await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            await transaction.CommitAsync(token).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SaveInFlightAsync(MqttInFlightState state, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        return _store.RunAsync(async (connection, token) =>
        {
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            await ExecuteAsync(
                connection,
                transaction,
                $"DELETE FROM {_store.Tables.QualifiedInFlightOutbound}; DELETE FROM {_store.Tables.QualifiedInFlightInbound};",
                token).ConfigureAwait(false);

            using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText = $"INSERT INTO {_store.Tables.QualifiedInFlightOutbound} (Stage, Version, Packet) VALUES (@stage, @version, @packet);";
                var stage = AddParameter(insert, "@stage");
                var version = AddParameter(insert, "@version");
                var packet = AddParameter(insert, "@packet");
                foreach (var entry in state.Outbound)
                {
                    stage.Value = (int)entry.Stage;
                    version.Value = (int)entry.Packet.ProtocolVersion;
                    packet.Value = PacketBlob.Encode(entry.Packet);
                    await insert.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
            }

            using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText =
                    $"""
                    IF NOT EXISTS (SELECT 1 FROM {_store.Tables.QualifiedInFlightInbound} WHERE PacketId = @id)
                        INSERT INTO {_store.Tables.QualifiedInFlightInbound} (PacketId) VALUES (@id);
                    """;
                var id = AddParameter(insert, "@id");
                foreach (var packetId in state.InboundExactlyOnce)
                {
                    id.Value = (int)packetId;
                    await insert.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
            }

            await transaction.CommitAsync(token).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<MqttInFlightState?> LoadInFlightAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(async (connection, token) =>
        {
            var outbound = new List<MqttInFlightPublish>();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT Stage, Version, Packet FROM {_store.Tables.QualifiedInFlightOutbound} ORDER BY Seq;";
                using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    var stage = (MqttInFlightStage)reader.GetInt32(0);
                    var version = (MqttProtocolVersion)reader.GetInt32(1);
                    var blob = await reader.GetFieldValueAsync<byte[]>(2, token).ConfigureAwait(false);
                    outbound.Add(new MqttInFlightPublish(PacketBlob.Decode(blob, version), stage));
                }
            }

            var inbound = new List<ushort>();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT PacketId FROM {_store.Tables.QualifiedInFlightInbound} ORDER BY PacketId;";
                using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    inbound.Add((ushort)reader.GetInt32(0));
                }
            }

            return outbound.Count == 0 && inbound.Count == 0
                ? null
                : new MqttInFlightState(outbound, inbound);
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(async (connection, token) =>
        {
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            await ExecuteAsync(
                connection,
                transaction,
                $"DELETE FROM {_store.Tables.QualifiedSubscriptions}; DELETE FROM {_store.Tables.QualifiedInFlightOutbound}; DELETE FROM {_store.Tables.QualifiedInFlightInbound};",
                token).ConfigureAwait(false);
            await transaction.CommitAsync(token).ConfigureAwait(false);
        }, cancellationToken);

    /// <inheritdoc />
    public void Dispose() => _store.Dispose();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private static string BuildSchema(SqlServerTables tables)
    {
        var schema = SqlServerTables.EscapeLiteral(tables.SchemaName);
        var subscriptions = SqlServerTables.EscapeLiteral(tables.Subscriptions);
        var outbound = SqlServerTables.EscapeLiteral(tables.InFlightOutbound);
        var inbound = SqlServerTables.EscapeLiteral(tables.InFlightInbound);
        return
            $"""
            IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'{schema}' AND t.name = N'{subscriptions}')
            BEGIN
                CREATE TABLE {tables.QualifiedSubscriptions} (Topic nvarchar(1024) NOT NULL PRIMARY KEY, Options tinyint NOT NULL);
            END;

            IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'{schema}' AND t.name = N'{outbound}')
            BEGIN
                CREATE TABLE {tables.QualifiedInFlightOutbound} (Seq bigint IDENTITY(1,1) NOT NULL PRIMARY KEY, Stage int NOT NULL, Version int NOT NULL, Packet varbinary(max) NOT NULL);
            END;

            IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'{schema}' AND t.name = N'{inbound}')
            BEGIN
                CREATE TABLE {tables.QualifiedInFlightInbound} (PacketId int NOT NULL PRIMARY KEY);
            END;
            """;
    }

    private async ValueTask UpsertSubscriptionsAsync(SqlConnection connection, SqlTransaction transaction, IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            UPDATE {_store.Tables.QualifiedSubscriptions} SET Options = @options WHERE Topic = @topic;
            IF @@ROWCOUNT = 0
                INSERT INTO {_store.Tables.QualifiedSubscriptions} (Topic, Options) VALUES (@topic, @options);
            """;
        var topic = AddParameter(command, "@topic");
        var options = AddParameter(command, "@options");
        foreach (var filter in topicFilters)
        {
            topic.Value = filter.Topic;
            options.Value = filter.ToSubscriptionOptions();
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask ExecuteAsync(SqlConnection connection, SqlTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static SqlParameter AddParameter(SqlCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
        return parameter;
    }
}
