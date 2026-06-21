namespace Pulse.Mqtt.DependencyInjection;

/// <summary>Optional threshold policy for <see cref="PulseMqttHealthCheck"/>.</summary>
public sealed class PulseMqttHealthCheckOptions
{
    /// <summary>Queue depth that degrades an otherwise healthier result. Must be positive when set.</summary>
    public int? DegradedOfflineQueueDepthThreshold { get; set; }

    /// <summary>Queue depth that makes the result unhealthy. Must be positive when set.</summary>
    public int? UnhealthyOfflineQueueDepthThreshold { get; set; }

    /// <summary>Dropped-publish count that degrades an otherwise healthier result. Must be positive when set.</summary>
    public long? DegradedOfflineQueueDroppedCountThreshold { get; set; }

    /// <summary>Dropped-publish count that makes the result unhealthy. Must be positive when set.</summary>
    public long? UnhealthyOfflineQueueDroppedCountThreshold { get; set; }

    /// <summary>Pending subscribe plus unsubscribe count that degrades an otherwise healthier result. Must be positive when set.</summary>
    public int? DegradedPendingSubscriptionOperationsThreshold { get; set; }

    /// <summary>Pending subscribe plus unsubscribe count that makes the result unhealthy. Must be positive when set.</summary>
    public int? UnhealthyPendingSubscriptionOperationsThreshold { get; set; }

    internal PulseMqttHealthCheckOptions Snapshot()
    {
        Validate();
        return new PulseMqttHealthCheckOptions
        {
            DegradedOfflineQueueDepthThreshold = DegradedOfflineQueueDepthThreshold,
            UnhealthyOfflineQueueDepthThreshold = UnhealthyOfflineQueueDepthThreshold,
            DegradedOfflineQueueDroppedCountThreshold = DegradedOfflineQueueDroppedCountThreshold,
            UnhealthyOfflineQueueDroppedCountThreshold = UnhealthyOfflineQueueDroppedCountThreshold,
            DegradedPendingSubscriptionOperationsThreshold = DegradedPendingSubscriptionOperationsThreshold,
            UnhealthyPendingSubscriptionOperationsThreshold = UnhealthyPendingSubscriptionOperationsThreshold,
        };
    }

    private void Validate()
    {
        ValidatePositive(DegradedOfflineQueueDepthThreshold, nameof(DegradedOfflineQueueDepthThreshold));
        ValidatePositive(UnhealthyOfflineQueueDepthThreshold, nameof(UnhealthyOfflineQueueDepthThreshold));
        ValidatePositive(DegradedOfflineQueueDroppedCountThreshold, nameof(DegradedOfflineQueueDroppedCountThreshold));
        ValidatePositive(UnhealthyOfflineQueueDroppedCountThreshold, nameof(UnhealthyOfflineQueueDroppedCountThreshold));
        ValidatePositive(DegradedPendingSubscriptionOperationsThreshold, nameof(DegradedPendingSubscriptionOperationsThreshold));
        ValidatePositive(UnhealthyPendingSubscriptionOperationsThreshold, nameof(UnhealthyPendingSubscriptionOperationsThreshold));

        ValidateOrder(
            DegradedOfflineQueueDepthThreshold,
            UnhealthyOfflineQueueDepthThreshold,
            nameof(DegradedOfflineQueueDepthThreshold),
            nameof(UnhealthyOfflineQueueDepthThreshold));
        ValidateOrder(
            DegradedOfflineQueueDroppedCountThreshold,
            UnhealthyOfflineQueueDroppedCountThreshold,
            nameof(DegradedOfflineQueueDroppedCountThreshold),
            nameof(UnhealthyOfflineQueueDroppedCountThreshold));
        ValidateOrder(
            DegradedPendingSubscriptionOperationsThreshold,
            UnhealthyPendingSubscriptionOperationsThreshold,
            nameof(DegradedPendingSubscriptionOperationsThreshold),
            nameof(UnhealthyPendingSubscriptionOperationsThreshold));
    }

    private static void ValidatePositive(int? threshold, string parameterName)
    {
        if (threshold is <= 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, threshold, "Thresholds must be positive when set.");
        }
    }

    private static void ValidatePositive(long? threshold, string parameterName)
    {
        if (threshold is <= 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, threshold, "Thresholds must be positive when set.");
        }
    }

    private static void ValidateOrder(int? degraded, int? unhealthy, string degradedName, string unhealthyName)
    {
        if (degraded is not null && unhealthy is not null && degraded > unhealthy)
        {
            throw new ArgumentException(
                $"{degradedName} must be less than or equal to {unhealthyName}.",
                degradedName);
        }
    }

    private static void ValidateOrder(long? degraded, long? unhealthy, string degradedName, string unhealthyName)
    {
        if (degraded is not null && unhealthy is not null && degraded > unhealthy)
        {
            throw new ArgumentException(
                $"{degradedName} must be less than or equal to {unhealthyName}.",
                degradedName);
        }
    }
}
