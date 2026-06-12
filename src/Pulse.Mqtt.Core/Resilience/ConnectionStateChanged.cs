using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>A transition of the resilient connection's <see cref="ConnectionState"/>.</summary>
/// <param name="Previous">The state before the transition.</param>
/// <param name="Current">The state after the transition.</param>
/// <param name="Attempt">The connection attempt counter (0 on the first connect).</param>
/// <param name="Reason">A reason code populated on drops and faults, if available.</param>
public readonly record struct ConnectionStateChanged(
    ConnectionState Previous,
    ConnectionState Current,
    int Attempt,
    MqttReasonCode? Reason = null);
