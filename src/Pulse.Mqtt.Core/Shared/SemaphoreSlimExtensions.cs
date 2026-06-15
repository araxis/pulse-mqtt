namespace Pulse.Mqtt;

/// <summary>
/// Cancellation-safe acquisition helpers for <see cref="SemaphoreSlim"/>.
/// </summary>
internal static class SemaphoreSlimExtensions
{
    /// <summary>
    /// Acquires the semaphore, confining the linked <see cref="CancellationTokenSource"/> that
    /// <see cref="SemaphoreSlim.WaitAsync(CancellationToken)"/> builds for each contended wait to a
    /// per-call source disposed immediately. Passing a long-lived caller token straight to
    /// <c>WaitAsync</c> leaks one such registration onto it for every contended wait; across a
    /// high-volume, long-lived client those accumulate and throw an
    /// <see cref="ObjectDisposedException"/> avalanche when the token is finally cancelled. A free
    /// slot is taken synchronously, so the common (uncontended) path allocates nothing.
    /// </summary>
    public static async ValueTask WaitScopedAsync(this SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        if (semaphore.Wait(0, cancellationToken))
        {
            return;
        }

        using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await semaphore.WaitAsync(scoped.Token).ConfigureAwait(false);
    }

    /// <summary>
    /// The bounded-wait companion to <see cref="WaitScopedAsync(SemaphoreSlim, CancellationToken)"/>,
    /// returning <see langword="false"/> if <paramref name="timeout"/> elapses before a slot is free.
    /// </summary>
    public static async ValueTask<bool> WaitScopedAsync(this SemaphoreSlim semaphore, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (semaphore.Wait(0, cancellationToken))
        {
            return true;
        }

        using var scoped = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        return await semaphore.WaitAsync(timeout, scoped.Token).ConfigureAwait(false);
    }
}
