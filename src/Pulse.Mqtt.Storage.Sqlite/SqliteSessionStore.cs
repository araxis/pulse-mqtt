using Microsoft.Data.Sqlite;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Storage.Sqlite;

/// <summary>
/// A durable <see cref="ISessionStore"/> over SQLite. Subscriptions and the in-flight QoS state are
/// persisted so a persistent-session client resumes — re-subscribing and redelivering unfinished
/// exchanges — after a process restart, not just a reconnect.
/// </summary>
public sealed class SqliteSessionStore : ISessionStore, IAsyncDisposable, IDisposable
{
    private const string Schema =
        """
        CREATE TABLE IF NOT EXISTS Subscriptions (Topic TEXT PRIMARY KEY, Options INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS InFlightOutbound (Seq INTEGER PRIMARY KEY AUTOINCREMENT, Stage INTEGER NOT NULL, Version INTEGER NOT NULL, Packet BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS InFlightInbound (PacketId INTEGER PRIMARY KEY);
        """;

    private readonly SqliteStore _store;

    /// <summary>Opens (creating if needed) the session database at <paramref name="connectionString"/>.</summary>
    /// <param name="connectionString">
    /// A Microsoft.Data.Sqlite connection string, or a plain file path (e.g. <c>mqtt-session.db</c>).
    /// </param>
    public SqliteSessionStore(string connectionString)
    {
        _store = new SqliteStore(SqliteStore.Normalize(connectionString), Schema);
    }

    /// <inheritdoc />
    public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        return _store.RunAsync(connection =>
        {
            using var transaction = connection.BeginTransaction();
            Execute(connection, transaction, "DELETE FROM Subscriptions;");
            UpsertSubscriptions(connection, transaction, topicFilters);
            transaction.Commit();
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(connection =>
        {
            var filters = new List<MqttTopicFilter>();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Topic, Options FROM Subscriptions ORDER BY Topic;";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                filters.Add(MqttTopicFilter.FromSubscriptionOptions(reader.GetString(0), (byte)reader.GetInt64(1)));
            }

            return (IReadOnlyList<MqttTopicFilter>)filters;
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        return _store.RunAsync(connection =>
        {
            using var transaction = connection.BeginTransaction();
            UpsertSubscriptions(connection, transaction, topicFilters);
            transaction.Commit();
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveSubscriptionsAsync(IReadOnlyList<string> topics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topics);
        return _store.RunAsync(connection =>
        {
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM Subscriptions WHERE Topic = $topic;";
            var topic = AddParameter(command, "$topic");
            foreach (var value in topics)
            {
                topic.Value = value;
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SaveInFlightAsync(MqttInFlightState state, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        return _store.RunAsync(connection =>
        {
            using var transaction = connection.BeginTransaction();
            Execute(connection, transaction, "DELETE FROM InFlightOutbound; DELETE FROM InFlightInbound;");

            using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText = "INSERT INTO InFlightOutbound (Stage, Version, Packet) VALUES ($stage, $version, $packet);";
                var stage = AddParameter(insert, "$stage");
                var version = AddParameter(insert, "$version");
                var packet = AddParameter(insert, "$packet");
                foreach (var entry in state.Outbound)
                {
                    stage.Value = (long)(int)entry.Stage;
                    version.Value = (long)(int)entry.Packet.ProtocolVersion;
                    packet.Value = PacketBlob.Encode(entry.Packet);
                    insert.ExecuteNonQuery();
                }
            }

            using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText = "INSERT OR IGNORE INTO InFlightInbound (PacketId) VALUES ($id);";
                var id = AddParameter(insert, "$id");
                foreach (var packetId in state.InboundExactlyOnce)
                {
                    id.Value = (long)packetId;
                    insert.ExecuteNonQuery();
                }
            }

            transaction.Commit();
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<MqttInFlightState?> LoadInFlightAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(connection =>
        {
            var outbound = new List<MqttInFlightPublish>();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT Stage, Version, Packet FROM InFlightOutbound ORDER BY Seq;";
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var stage = (MqttInFlightStage)(int)reader.GetInt64(0);
                    var version = (MqttProtocolVersion)(int)reader.GetInt64(1);
                    outbound.Add(new MqttInFlightPublish(PacketBlob.Decode(reader.GetFieldValue<byte[]>(2), version), stage));
                }
            }

            var inbound = new List<ushort>();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT PacketId FROM InFlightInbound ORDER BY PacketId;";
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    inbound.Add((ushort)reader.GetInt64(0));
                }
            }

            return outbound.Count == 0 && inbound.Count == 0
                ? null
                : new MqttInFlightState(outbound, inbound);
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(connection =>
        {
            using var transaction = connection.BeginTransaction();
            Execute(connection, transaction, "DELETE FROM Subscriptions; DELETE FROM InFlightOutbound; DELETE FROM InFlightInbound;");
            transaction.Commit();
        }, cancellationToken);

    /// <inheritdoc />
    public void Dispose() => _store.Dispose();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private static void UpsertSubscriptions(SqliteConnection connection, SqliteTransaction transaction, IReadOnlyList<MqttTopicFilter> topicFilters)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT OR REPLACE INTO Subscriptions (Topic, Options) VALUES ($topic, $options);";
        var topic = AddParameter(command, "$topic");
        var options = AddParameter(command, "$options");
        foreach (var filter in topicFilters)
        {
            topic.Value = filter.Topic;
            options.Value = (long)filter.ToSubscriptionOptions();
            command.ExecuteNonQuery();
        }
    }

    private static void Execute(SqliteConnection connection, SqliteTransaction? transaction, string sql)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static SqliteParameter AddParameter(SqliteCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
        return parameter;
    }
}
