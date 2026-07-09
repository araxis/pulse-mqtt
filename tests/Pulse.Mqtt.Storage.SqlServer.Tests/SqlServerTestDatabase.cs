using Microsoft.Data.SqlClient;

namespace Pulse.Mqtt.Storage.SqlServer.Tests;

internal sealed class SqlServerTestDatabase : IAsyncDisposable
{
    public const string ConnectionStringVariable = "PULSE_MQTT_SQLSERVER";

    private static readonly string[] Suffixes =
    [
        "Queue",
        "InFlightInbound",
        "InFlightOutbound",
        "Subscriptions",
    ];

    private SqlServerTestDatabase(string connectionString, string schemaName, string tablePrefix)
    {
        ConnectionString = connectionString;
        SchemaName = schemaName;
        TablePrefix = tablePrefix;
        Options = new SqlServerStorageOptions
        {
            SchemaName = schemaName,
            TablePrefix = tablePrefix,
        };
    }

    public string ConnectionString { get; }

    public string SchemaName { get; }

    public string TablePrefix { get; }

    public SqlServerStorageOptions Options { get; }

    public static bool HasConnectionString => !string.IsNullOrWhiteSpace(ConfiguredConnectionString);

    private static string? ConfiguredConnectionString => Environment.GetEnvironmentVariable(ConnectionStringVariable);

    public static SqlServerTestDatabase Create()
    {
        var connectionString = ConfiguredConnectionString;
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException($"Set {ConnectionStringVariable} to run SQL Server storage tests.");
        }

        return new SqlServerTestDatabase(
            connectionString,
            schemaName: "dbo",
            tablePrefix: "PulseMqttTest" + Guid.NewGuid().ToString("N"));
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync().ConfigureAwait(false);

            foreach (var suffix in Suffixes)
            {
                await using var command = connection.CreateCommand();
                var tableName = TablePrefix + suffix;
                command.CommandText =
                    $"""
                    IF OBJECT_ID(N'{EscapeLiteral(SchemaName)}.{EscapeLiteral(tableName)}', N'U') IS NOT NULL
                        DROP TABLE {QuoteIdentifier(SchemaName)}.{QuoteIdentifier(tableName)};
                    """;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        catch (SqlException)
        {
        }
        finally
        {
            SqlConnection.ClearAllPools();
        }
    }

    private static string QuoteIdentifier(string identifier) => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";

    private static string EscapeLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);
}
