using Microsoft.Data.Sqlite;

namespace Pulse.Mqtt.Storage.Sqlite.Tests;

/// <summary>A unique temp database file path that cleans up its file (and WAL sidecars) on dispose.</summary>
internal sealed class TempDatabase : IDisposable
{
    public string Path { get; } =
        System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"pulse-mqtt-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        // Force pooled connections closed so the file handle is released and the file can be deleted.
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { string.Empty, "-wal", "-shm" })
        {
            try
            {
                File.Delete(Path + suffix);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
