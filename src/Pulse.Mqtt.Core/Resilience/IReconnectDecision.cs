namespace Pulse.Mqtt.Resilience;

/// <summary>
/// Classifies a failed connection attempt as retryable or final. Swapping this lets callers, for
/// example, treat an authorization failure as transient when a token refreshes out of band.
/// </summary>
public interface IReconnectDecision
{
    /// <summary>Returns whether attempt <paramref name="attempt"/> (which failed with <paramref name="error"/>) should be retried.</summary>
    bool ShouldRetry(int attempt, Exception error);
}
