using Polly;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Resilience.Polly;

/// <summary>
/// An <see cref="IReconnectStrategy"/> backed by a Polly <see cref="ResiliencePipeline"/>: the
/// pipeline owns retry counts, delays, jitter, and circuit breaking. Each connect attempt is
/// reported through the supervisor's context.
/// </summary>
/// <remarks>
/// Configure the pipeline so it does <b>not</b> handle <see cref="TerminalMqttConnectException"/>
/// or <see cref="MqttConnectRejectedException"/> for final reasons — those must escape so the
/// supervisor faults instead of retrying an unwinnable connection forever.
/// </remarks>
public sealed class PollyReconnectStrategy : IReconnectStrategy
{
    private readonly ResiliencePipeline _pipeline;

    /// <summary>Creates a strategy that drives reconnection through <paramref name="pipeline"/>.</summary>
    public PollyReconnectStrategy(ResiliencePipeline pipeline)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        _pipeline = pipeline;
    }

    /// <inheritdoc />
    public async Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connectOnce);
        ArgumentNullException.ThrowIfNull(context);

        var attempt = 0;
        await _pipeline.ExecuteAsync(
            async token =>
            {
                var current = Interlocked.Increment(ref attempt);
                context.OnAttemptStarting(current);
                try
                {
                    await connectOnce(token).ConfigureAwait(false);
                }
                catch (Exception error) when (error is not OperationCanceledException)
                {
                    context.OnAttemptFailed(current, error);
                    throw;
                }
            },
            cancellationToken).ConfigureAwait(false);
    }
}
