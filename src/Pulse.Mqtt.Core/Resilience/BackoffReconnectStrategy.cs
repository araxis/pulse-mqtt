namespace Pulse.Mqtt.Resilience;

/// <summary>
/// The default reconnect strategy: exponential backoff with full jitter, capped, retrying
/// indefinitely by default. It consults an <see cref="IReconnectDecision"/> to stop on terminal
/// failures and rethrows <see cref="TerminalMqttConnectException"/> so the supervisor faults.
/// </summary>
public sealed class BackoffReconnectStrategy : IReconnectStrategy
{
    private readonly BackoffOptions _options;
    private readonly IReconnectDecision _decision;
    private readonly Random _random;

    /// <summary>Creates a strategy with the given bounds and retry classification.</summary>
    /// <param name="options">The backoff bounds and attempt cap.</param>
    /// <param name="decision">Classifies failures as retryable or final.</param>
    /// <param name="random">The jitter source; defaults to <see cref="Random.Shared"/>.</param>
    public BackoffReconnectStrategy(BackoffOptions options, IReconnectDecision decision, Random? random = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(decision);

        _options = options;
        _decision = decision;
        _random = random ?? Random.Shared;
    }

    /// <inheritdoc />
    public async Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connectOnce);
        ArgumentNullException.ThrowIfNull(context);

        for (var attempt = 1; ; attempt++)
        {
            context.OnAttemptStarting(attempt);
            try
            {
                await connectOnce(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (TerminalMqttConnectException)
            {
                throw;
            }
            catch (Exception error)
            {
                context.OnAttemptFailed(attempt, error);

                var attemptsExhausted = _options.MaxAttempts is { } max && attempt >= max;
                if (attemptsExhausted || !_decision.ShouldRetry(attempt, error))
                {
                    throw new TerminalMqttConnectException("Reconnection gave up.", error);
                }

                var delay = Backoff.Compute(attempt, _options, _random);
                await Task.Delay(delay, context.Time, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
