namespace Pulse.Mqtt.Resilience;

/// <summary>Settings for <see cref="BackoffReconnectStrategy"/>.</summary>
public sealed record BackoffOptions
{
    /// <summary>The base delay used for the first retry; doubles each subsequent attempt.</summary>
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>The maximum delay the exponential growth is capped at.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The maximum number of attempts, or <see langword="null"/> to retry indefinitely.</summary>
    public int? MaxAttempts { get; init; }
}
