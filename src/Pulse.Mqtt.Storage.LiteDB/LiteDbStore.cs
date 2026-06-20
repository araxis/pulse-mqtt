using LiteDB;

namespace Pulse.Mqtt.Storage.LiteDB;

/// <summary>
/// Shared plumbing for the LiteDB stores, held by composition so no LiteDB type leaks into Pulse's
/// public surface. The store owns one database handle for its lifetime and serializes every access
/// through an async gate so the higher-level count and overflow bookkeeping stays authoritative.
/// </summary>
internal sealed class LiteDbStore : IAsyncDisposable, IDisposable
{
    private const long LiteDbPageSize = 8192;

    private readonly LiteDatabase _database;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    /// <summary>Opens the store at <paramref name="connectionString"/>.</summary>
    /// <exception cref="LiteDbStorageException">The database cannot be opened or initialized.</exception>
    public LiteDbStore(string connectionString)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        var normalized = Normalize(connectionString);
        try
        {
            ValidateExistingFile(connectionString);
            _database = new LiteDatabase(normalized);
            _database.GetCollectionNames();
        }
        catch (LiteException ex)
        {
            throw new LiteDbStorageException(
                $"Failed to open the LiteDB store '{connectionString}'. The database may be missing, locked, or not a valid LiteDB file.",
                ex);
        }
        catch (IOException ex)
        {
            throw new LiteDbStorageException(
                $"Failed to open the LiteDB store '{connectionString}'. The database may be missing, locked, or not a valid LiteDB file.",
                ex);
        }
        catch
        {
            _database?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Accepts a bare file path as a convenience, expanding it to a LiteDB
    /// <c>Filename=…</c> connection string when it is not already one.
    /// </summary>
    public static string Normalize(string connectionStringOrPath)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionStringOrPath);
        return connectionStringOrPath.Contains('=', StringComparison.Ordinal)
            ? connectionStringOrPath
            : $"Filename={connectionStringOrPath}";
    }

    private static void ValidateExistingFile(string connectionStringOrPath)
    {
        var path = TryGetFilePath(connectionStringOrPath);
        if (path is null || !File.Exists(path))
        {
            return;
        }

        var length = new FileInfo(path).Length;
        if (length > 0 && length < LiteDbPageSize)
        {
            throw new LiteDbStorageException(
                $"Failed to open the LiteDB store '{connectionStringOrPath}'. The database file is too small to be a valid LiteDB file.",
                new InvalidDataException("The database file is smaller than a LiteDB page."));
        }
    }

    private static string? TryGetFilePath(string connectionStringOrPath)
    {
        if (!connectionStringOrPath.Contains('=', StringComparison.Ordinal))
        {
            return connectionStringOrPath;
        }

        foreach (var part in connectionStringOrPath.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            if (separator < 0)
            {
                continue;
            }

            var key = part[..separator];
            if (key.Equals("Filename", StringComparison.OrdinalIgnoreCase) ||
                key.Equals("FileName", StringComparison.OrdinalIgnoreCase))
            {
                return part[(separator + 1)..];
            }
        }

        return null;
    }

    /// <summary>Runs <paramref name="work"/> against the database under the serialization gate.</summary>
    public async ValueTask<T> RunAsync<T>(Func<LiteDatabase, T> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await AcquireGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return work(_database);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Runs <paramref name="work"/> against the database under the serialization gate.</summary>
    public async ValueTask RunAsync(Action<LiteDatabase> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await AcquireGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            work(_database);
        }
        finally
        {
            _gate.Release();
        }
    }

    // SemaphoreSlim.WaitAsync(token) builds an internal linked CancellationTokenSource on the
    // supplied token for every contended wait; take the gate synchronously when free and confine
    // any real wait to a per-call source disposed immediately.
    private async ValueTask AcquireGateAsync(CancellationToken cancellationToken)
    {
        if (_gate.Wait(0, cancellationToken))
        {
            return;
        }

        using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await _gate.WaitAsync(scoped.Token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _database.Dispose();
        _gate.Dispose();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
