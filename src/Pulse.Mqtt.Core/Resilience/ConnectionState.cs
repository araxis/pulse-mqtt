namespace Pulse.Mqtt.Resilience;

/// <summary>The observable lifecycle state of a resilient connection.</summary>
public enum ConnectionState
{
    /// <summary>Initial state, or after a graceful stop.</summary>
    Disconnected,

    /// <summary>The first connection attempt is in progress.</summary>
    Connecting,

    /// <summary>A session is live.</summary>
    Connected,

    /// <summary>The connection was lost and is being restored.</summary>
    Reconnecting,

    /// <summary>Backing off between connection attempts.</summary>
    WaitingRetry,

    /// <summary>Gave up on a terminal failure (for example, not authorized). Restart is required.</summary>
    Faulted,

    /// <summary>Stopped at the caller's request.</summary>
    Stopped,
}
