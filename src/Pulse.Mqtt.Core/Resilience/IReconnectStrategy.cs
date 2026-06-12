namespace Pulse.Mqtt.Resilience;

/// <summary>
/// One connection attempt: connect the transport and perform the handshake. Throws on failure —
/// the strategy and the <see cref="IReconnectDecision"/> classify whether to retry.
/// </summary>
public delegate Task ConnectOnceAsync(CancellationToken cancellationToken);

/// <summary>
/// Owns the (re)connection loop. The default backs off; an alternative may wrap a Polly pipeline
/// or attempt exactly once. This is a swap point: the supervisor delegates connecting-with-retry
/// to whichever strategy is registered.
/// </summary>
public interface IReconnectStrategy
{
    /// <summary>
    /// Drives reconnection: invokes <paramref name="connectOnce"/> repeatedly per its own policy
    /// until success (returns), a terminal failure (rethrows), or cancellation. Reports each
    /// attempt through <paramref name="context"/>.
    /// </summary>
    Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken cancellationToken);
}

/// <summary>The supervisor-provided context a strategy reports attempts to and takes time from.</summary>
public interface IReconnectContext
{
    /// <summary>The current attempt number (1-based).</summary>
    int Attempt { get; }

    /// <summary>The time source for all delays, so reconnection is deterministically testable.</summary>
    TimeProvider Time { get; }

    /// <summary>Signals that attempt <paramref name="attempt"/> is starting.</summary>
    void OnAttemptStarting(int attempt);

    /// <summary>Signals that attempt <paramref name="attempt"/> failed with <paramref name="error"/>.</summary>
    void OnAttemptFailed(int attempt, Exception error);
}
