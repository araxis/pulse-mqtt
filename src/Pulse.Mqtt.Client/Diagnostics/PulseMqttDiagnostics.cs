using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Pulse.Mqtt.Client;

/// <summary>
/// The library's telemetry sources. Subscribe an OpenTelemetry (or any) listener to the
/// <c>Pulse.Mqtt</c> activity source and meter to collect traces and metrics; with no listener
/// attached the instrumentation costs almost nothing.
/// </summary>
public static class PulseMqttDiagnostics
{
    /// <summary>The name of the activity source and meter.</summary>
    public const string SourceName = "Pulse.Mqtt";

    /// <summary>Spans for connect, publish, and receive operations.</summary>
    public static ActivitySource ActivitySource { get; } = new(SourceName);

    internal static Meter Meter { get; } = new(SourceName);

    internal static Counter<long> ConnectAttempts { get; } =
        Meter.CreateCounter<long>("pulse.mqtt.client.connect.attempts", description: "Connection attempts, including retries.");

    internal static Counter<long> StateTransitions { get; } =
        Meter.CreateCounter<long>("pulse.mqtt.client.state.transitions", description: "Connection state transitions.");

    internal static Counter<long> MessagesPublished { get; } =
        Meter.CreateCounter<long>("pulse.mqtt.client.messages.published", description: "Publishes by disposition (delivered, queued, dropped).");

    internal static Counter<long> MessagesReceived { get; } =
        Meter.CreateCounter<long>("pulse.mqtt.client.messages.received", description: "Application messages received.");
}
