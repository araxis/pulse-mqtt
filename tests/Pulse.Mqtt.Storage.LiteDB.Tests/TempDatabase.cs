namespace Pulse.Mqtt.Storage.LiteDB.Tests;

/// <summary>A unique temp database file path that cleans up its file and LiteDB sidecars on dispose.</summary>
internal sealed class TempDatabase : IDisposable
{
    public string Path { get; } =
        System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"pulse-mqtt-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var candidate in Candidates())
        {
            try
            {
                File.Delete(candidate);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    private IEnumerable<string> Candidates()
    {
        yield return Path;

        var directory = System.IO.Path.GetDirectoryName(Path);
        var name = System.IO.Path.GetFileNameWithoutExtension(Path);
        var extension = System.IO.Path.GetExtension(Path);
        if (directory is not null)
        {
            yield return System.IO.Path.Combine(directory, $"{name}-log{extension}");
        }
    }
}
