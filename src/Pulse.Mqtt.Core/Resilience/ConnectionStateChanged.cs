using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>A transition of the resilient connection's <see cref="ConnectionState"/>.</summary>
public readonly record struct ConnectionStateChanged
{
    /// <summary>Creates a state transition.</summary>
    public ConnectionStateChanged(
        ConnectionState Previous,
        ConnectionState Current,
        int Attempt,
        MqttReasonCode? Reason = null)
    {
        this.Previous = Previous;
        this.Current = Current;
        this.Attempt = Attempt;
        this.Reason = Reason;
    }

    /// <summary>The state before the transition.</summary>
    public ConnectionState Previous { get; init; }

    /// <summary>The state after the transition.</summary>
    public ConnectionState Current { get; init; }

    /// <summary>The connection attempt counter (0 on the first connect).</summary>
    public int Attempt { get; init; }

    /// <summary>A reason code populated on drops and faults, if available.</summary>
    public MqttReasonCode? Reason { get; init; }

    /// <summary>The broker's human-readable reason string, when one is available.</summary>
    public string? ReasonString { get; init; }

    /// <summary>An alternate server reference sent by the broker, when one is available.</summary>
    public string? ServerReference { get; init; }

    /// <summary>The exception that triggered the transition, when one is known.</summary>
    public Exception? Error { get; init; }

    /// <summary>Deconstructs the original state transition fields.</summary>
    public void Deconstruct(
        out ConnectionState Previous,
        out ConnectionState Current,
        out int Attempt,
        out MqttReasonCode? Reason)
    {
        Previous = this.Previous;
        Current = this.Current;
        Attempt = this.Attempt;
        Reason = this.Reason;
    }
}
