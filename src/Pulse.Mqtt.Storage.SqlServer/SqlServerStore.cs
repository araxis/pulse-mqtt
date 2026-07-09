using Microsoft.Data.SqlClient;

namespace Pulse.Mqtt.Storage.SqlServer;

internal sealed class SqlServerStore : IAsyncDisposable, IDisposable
{
    private readonly string _connectionString;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public SqlServerStore(string connectionString, SqlServerStorageOptions? options, string schema)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);
        ArgumentException.ThrowIfNullOrEmpty(schema);

        _connectionString = connectionString;
        Tables = SqlServerTables.Create(options);

        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            Execute(connection, BuildSchemaHeader());
            Execute(connection, schema);
        }
        catch (SqlException ex)
        {
            throw new SqlServerStorageException(
                "Failed to open or initialize the SQL Server store. Check the connection string, database availability, and schema permissions.",
                ex);
        }
    }

    public SqlServerTables Tables { get; }

    public async ValueTask<T> RunAsync<T>(Func<SqlConnection, CancellationToken, ValueTask<T>> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await AcquireGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return await work(connection, cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            throw new SqlServerStorageException("A SQL Server storage operation failed.", ex);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask RunAsync(Func<SqlConnection, CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        await RunAsync(async ValueTask<bool> (connection, token) =>
        {
            await work(connection, token).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    public long ExecuteScalarLong(string sql)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteScalar() switch
            {
                long value => value,
                int value => value,
                decimal value => (long)value,
                _ => 0,
            };
        }
        catch (SqlException ex)
        {
            throw new SqlServerStorageException("A SQL Server storage operation failed.", ex);
        }
    }

    public int ExecuteNonQuery(string sql)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteNonQuery();
        }
        catch (SqlException ex)
        {
            throw new SqlServerStorageException("A SQL Server storage operation failed.", ex);
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _gate.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    private async ValueTask AcquireGateAsync(CancellationToken cancellationToken)
    {
        if (_gate.Wait(0, cancellationToken))
        {
            return;
        }

        using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await _gate.WaitAsync(scoped.Token).ConfigureAwait(false);
    }

    private string BuildSchemaHeader()
    {
        var schemaLiteral = SqlServerTables.EscapeLiteral(Tables.SchemaName);
        var quotedSchema = SqlServerTables.QuoteIdentifier(Tables.SchemaName);
        return
            $"""
            IF SCHEMA_ID(N'{schemaLiteral}') IS NULL
                EXEC(N'CREATE SCHEMA {quotedSchema}');
            """;
    }

    private static void Execute(SqlConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
