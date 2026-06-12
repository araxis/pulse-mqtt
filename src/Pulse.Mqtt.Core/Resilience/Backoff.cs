namespace Pulse.Mqtt.Resilience;

/// <summary>Computes exponential backoff delays with full jitter.</summary>
public static class Backoff
{
    /// <summary>
    /// Returns a delay for <paramref name="attempt"/> (1-based): a uniform value in
    /// <c>[0, min(MaxDelay, BaseDelay * 2^(attempt-1))]</c>. The exponent is overflow-safe.
    /// </summary>
    /// <param name="attempt">The 1-based attempt number.</param>
    /// <param name="options">The backoff bounds.</param>
    /// <param name="random">The jitter source.</param>
    public static TimeSpan Compute(int attempt, BackoffOptions options, Random random)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(random);

        var capMs = options.MaxDelay.TotalMilliseconds;
        var baseMs = options.BaseDelay.TotalMilliseconds;

        var shift = attempt - 1;
        var factor = shift >= 31 ? double.PositiveInfinity : 1L << shift;
        var ceiling = Math.Min(capMs, baseMs * factor);

        return TimeSpan.FromMilliseconds(random.NextDouble() * ceiling);
    }
}
