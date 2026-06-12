namespace Pulse.Mqtt.Resilience;

/// <summary>
/// A strategy that attempts to connect exactly once and never retries. Use it when an outer system
/// owns the connection lifecycle.
/// </summary>
public sealed class NoReconnectStrategy : IReconnectStrategy
{
    /// <inheritdoc />
    public async Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connectOnce);
        ArgumentNullException.ThrowIfNull(context);

        context.OnAttemptStarting(1);
        try
        {
            await connectOnce(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error)
        {
            context.OnAttemptFailed(1, error);
            throw;
        }
    }
}
