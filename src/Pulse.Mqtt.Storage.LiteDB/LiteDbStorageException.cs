namespace Pulse.Mqtt.Storage.LiteDB;

/// <summary>
/// Thrown when a LiteDB store cannot be opened or a stored row cannot be read back — for example a
/// missing, locked, or corrupt database file, or a truncated packet blob. The cause is always
/// surfaced clearly rather than swallowed.
/// </summary>
public sealed class LiteDbStorageException : Exception
{
    /// <summary>Creates the exception with a message.</summary>
    public LiteDbStorageException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and the underlying cause.</summary>
    public LiteDbStorageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
