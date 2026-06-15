using Microsoft.Data.Sqlite;

namespace Pulse.Mqtt.Storage.Sqlite;

/// <summary>
/// Shared plumbing for the SQLite stores, held by composition so no Microsoft.Data.Sqlite type
/// leaks into Pulse's public surface: owns one open connection for the store's lifetime, serializes
/// every access through an async gate (a Microsoft.Data.Sqlite connection is not safe for concurrent
/// commands), and applies the pragmas and schema once at construction.
/// </summary>
internal sealed class SqliteStore : IAsyncDisposable, IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    /// <summary>Opens the store at <paramref name="connectionString"/> and applies <paramref name="schema"/>.</summary>
    /// <exception cref="SqliteStorageException">The database cannot be opened or initialized.</exception>
    public SqliteStore(string connectionString, string schema)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);
        ArgumentException.ThrowIfNullOrEmpty(schema);
        _connection = new SqliteConnection(connectionString);

        try
        {
            _connection.Open();

            // A rollback journal (the default) leaves a single file at rest; busy_timeout makes a
            // second connection to the same file wait for a lock instead of failing fast.
            Execute("PRAGMA busy_timeout=5000;");
            Execute(schema);
        }
        catch (SqliteException ex)
        {
            _connection.Dispose();
            throw new SqliteStorageException(
                $"Failed to open the SQLite store '{connectionString}'. The database may be missing, locked, or not a valid SQLite file.",
                ex);
        }
        catch
        {
            // Any other failure during initialization still must not leak the open connection.
            _connection.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Accepts a bare file path as a convenience, expanding it to a Microsoft.Data.Sqlite
    /// <c>Data Source=…</c> connection string when it is not already one.
    /// </summary>
    public static string Normalize(string connectionStringOrPath)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionStringOrPath);
        return connectionStringOrPath.Contains('=', StringComparison.Ordinal)
            ? connectionStringOrPath
            : $"Data Source={connectionStringOrPath}";
    }

    /// <summary>Runs <paramref name="work"/> against the connection under the serialization gate.</summary>
    public async ValueTask<T> RunAsync<T>(Func<SqliteConnection, T> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await AcquireGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return work(_connection);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Runs <paramref name="work"/> against the connection under the serialization gate.</summary>
    public async ValueTask RunAsync(Action<SqliteConnection> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await AcquireGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            work(_connection);
        }
        finally
        {
            _gate.Release();
        }
    }

    // SemaphoreSlim.WaitAsync(token) builds an internal linked CancellationTokenSource on the supplied
    // token for every contended wait; a long-lived caller token over a high-volume store accumulates
    // those registrations until cancelling the token throws an ObjectDisposedException avalanche. Take
    // the gate synchronously when free and confine any real wait to a per-call source disposed at once.
    private async ValueTask AcquireGateAsync(CancellationToken cancellationToken)
    {
        if (_gate.Wait(0, cancellationToken))
        {
            return;
        }

        using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await _gate.WaitAsync(scoped.Token).ConfigureAwait(false);
    }

    /// <summary>Executes a non-query statement on the connection. For construction-time use only.</summary>
    public void Execute(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    /// <summary>Returns the single scalar value of <paramref name="sql"/>. For construction-time use only.</summary>
    public long ExecuteScalarLong(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar() is long value ? value : 0;
    }

    /// <summary>Creates a command on the connection. For construction-time use only.</summary>
    public SqliteCommand CreateCommand() => _connection.CreateCommand();

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _connection.Dispose();
        _gate.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _connection.DisposeAsync().ConfigureAwait(false);
        _gate.Dispose();
    }
}
