using System.Reflection;
using Pulse.Mqtt;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

/// <summary>
/// <see cref="SemaphoreSlim.WaitAsync(CancellationToken)"/> builds an internal linked
/// <see cref="CancellationTokenSource"/> on the supplied token for every contended wait. Passing a
/// long-lived token straight in leaks one registration onto it per contended wait, so over a
/// high-volume, long-lived client they accumulate until cancelling the token throws an
/// <see cref="AggregateException"/> of <see cref="ObjectDisposedException"/> — the failure a 24h soak
/// surfaced. <see cref="SemaphoreSlimExtensions.WaitScopedAsync(SemaphoreSlim, CancellationToken)"/>
/// confines those registrations to a per-call source, so the caller's token never accumulates them.
/// </summary>
public sealed class SemaphoreSlimExtensionsTests
{
    [Fact]
    public async Task WaitScopedAsync_does_not_accumulate_registrations_under_repeated_contention()
    {
        using var semaphore = new SemaphoreSlim(1, 1);
        using var longLived = new CancellationTokenSource();

        for (var i = 0; i < 10_000; i++)
        {
            await semaphore.WaitScopedAsync(longLived.Token);                     // takes the free slot
            var contended = semaphore.WaitScopedAsync(longLived.Token).AsTask();  // count is 0: must wait
            semaphore.Release();                                                  // hands the slot over
            await contended;
            semaphore.Release();                                                  // restore one free slot
        }

        // Raw WaitAsync(token) would have left a growing pile of linked-CTS registrations here.
        CountRegistrations(longLived).ShouldBeLessThan(10);
        Should.NotThrow(longLived.Cancel);
    }

    [Fact]
    public async Task WaitScopedAsync_with_timeout_returns_false_when_the_slot_never_frees()
    {
        using var semaphore = new SemaphoreSlim(1, 1);
        await semaphore.WaitScopedAsync(CancellationToken.None); // hold the only slot

        var acquired = await semaphore.WaitScopedAsync(TimeSpan.FromMilliseconds(20), CancellationToken.None);

        acquired.ShouldBeFalse();
    }

    [Fact]
    public async Task WaitScopedAsync_honors_caller_cancellation_while_waiting()
    {
        using var semaphore = new SemaphoreSlim(1, 1);
        await semaphore.WaitScopedAsync(CancellationToken.None); // hold the only slot
        using var cancel = new CancellationTokenSource();

        var waiting = semaphore.WaitScopedAsync(cancel.Token).AsTask();
        await cancel.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(async () => await waiting);
    }

    private static int CountRegistrations(CancellationTokenSource cts)
    {
        var registrations = typeof(CancellationTokenSource)
            .GetField("_registrations", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(cts);
        if (registrations is null)
        {
            return 0;
        }

        var node = registrations.GetType().GetField("Callbacks", BindingFlags.Public | BindingFlags.Instance)!.GetValue(registrations);
        var nextField = node?.GetType().GetField("Next", BindingFlags.Public | BindingFlags.Instance);

        var count = 0;
        while (node is not null && nextField is not null)
        {
            count++;
            node = nextField.GetValue(node);
        }

        return count;
    }
}
