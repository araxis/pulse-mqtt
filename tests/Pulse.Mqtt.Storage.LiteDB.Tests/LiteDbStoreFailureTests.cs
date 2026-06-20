using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Storage.LiteDB.Tests;

public sealed class LiteDbStoreFailureTests
{
    [Fact]
    public async Task Opening_a_corrupt_file_fails_with_a_clear_error()
    {
        using var database = new TempDatabase();
        await File.WriteAllBytesAsync(database.Path, [0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0x42, 0x42]);

        var error = Should.Throw<LiteDbStorageException>(() => new LiteDbSessionStore(database.Path));
        error.Message.ShouldContain(database.Path);
        error.InnerException.ShouldNotBeNull();
    }

    [Fact]
    public void A_blank_path_is_rejected()
    {
        Should.Throw<ArgumentException>(() => new LiteDbSessionStore(string.Empty));
    }

    [Fact]
    public async Task Operations_after_dispose_throw_object_disposed()
    {
        using var database = new TempDatabase();
        var store = new LiteDbMessageStore(database.Path, new OfflineQueueOptions { Capacity = 4 });
        await store.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(() => store.PeekAsync(CancellationToken.None).AsTask());
    }
}
