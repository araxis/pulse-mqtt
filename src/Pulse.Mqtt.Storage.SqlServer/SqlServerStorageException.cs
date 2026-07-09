namespace Pulse.Mqtt.Storage.SqlServer;

/// <summary>
/// Thrown when a SQL Server store cannot be opened, initialized, or read back — for example an
/// unavailable database, insufficient schema permissions, or a malformed stored packet blob.
/// </summary>
public sealed class SqlServerStorageException : Exception
{
    /// <summary>Creates the exception with a message.</summary>
    public SqlServerStorageException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and the underlying cause.</summary>
    public SqlServerStorageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
