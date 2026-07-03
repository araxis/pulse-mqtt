using System.Collections.Concurrent;
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

    internal static Counter<long> AcknowledgedRouteBackpressure { get; } =
        Meter.CreateCounter<long>("pulse.mqtt.client.route.acknowledged.backpressure", description: "Times a full acknowledged route blocked the shared inbound sink, tagged by route.");

    internal static Histogram<double> PublishDuration { get; } =
        Meter.CreateHistogram<double>("pulse.mqtt.client.publish.duration", unit: "s", description: "Time spent publishing, by disposition.");

    internal static Histogram<double> ConnectDuration { get; } =
        Meter.CreateHistogram<double>("pulse.mqtt.client.connect.duration", unit: "s", description: "Time per connection attempt, by outcome.");

    // Live clients register a probe so the observable instruments below can report one measurement
    // per client without each client adding a duplicate instrument to the shared meter.
    private static readonly ConcurrentDictionary<IOfflineQueueProbe, byte> Probes = new();

    internal static ObservableGauge<long> OfflineQueueDepth { get; } =
        Meter.CreateObservableGauge("pulse.mqtt.client.offline.queue.depth", ObserveDepth, unit: "{message}", description: "Publishes currently waiting in the offline queue.");

    internal static ObservableCounter<long> OfflineQueueDropped { get; } =
        Meter.CreateObservableCounter("pulse.mqtt.client.offline.queue.dropped", ObserveDropped, unit: "{message}", description: "Publishes the offline-queue overflow policy has dropped.");

    /// <summary>Reports the current offline-queue depth and dropped count for one client.</summary>
    internal interface IOfflineQueueProbe
    {
        string ClientId { get; }

        long Depth { get; }

        long Dropped { get; }
    }

    internal static void RegisterOfflineQueue(IOfflineQueueProbe probe) => Probes.TryAdd(probe, 0);

    internal static void UnregisterOfflineQueue(IOfflineQueueProbe? probe)
    {
        if (probe is not null)
        {
            Probes.TryRemove(probe, out _);
        }
    }

    private static IEnumerable<Measurement<long>> ObserveDepth() => Observe(static probe => probe.Depth);

    private static IEnumerable<Measurement<long>> ObserveDropped() => Observe(static probe => probe.Dropped);

    // Reads one measurement per registered probe. A probe read can call a custom IMessageStore that
    // throws (a disposed durable store, transient I/O); that probe is skipped rather than allowed to
    // abort collection of the whole instrument, which would lose every other client's measurement.
    private static IEnumerable<Measurement<long>> Observe(Func<IOfflineQueueProbe, long> read)
    {
        foreach (var probe in Probes.Keys)
        {
            long value;
            string clientId;
            try
            {
                value = read(probe);
                clientId = probe.ClientId;
            }
            catch (Exception)
            {
                continue;
            }

            yield return new Measurement<long>(value, new KeyValuePair<string, object?>("client.id", clientId));
        }
    }
}
