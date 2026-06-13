namespace Pulse.Mqtt.Storage.Sqlite;

/// <summary>
/// Thrown when a SQLite store cannot be opened or a stored row cannot be read back — for example a
/// missing, locked, or corrupt database file, or a truncated packet blob. The cause is always
/// surfaced clearly rather than swallowed.
/// </summary>
public sealed class SqliteStorageException : Exception
{
    /// <summary>Creates the exception with a message.</summary>
    public SqliteStorageException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and the underlying cause.</summary>
    public SqliteStorageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
