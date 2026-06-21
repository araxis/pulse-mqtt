# Observability

Everything worth watching is exposed through the standard .NET primitives — `ILogger`,
`ActivitySource`, `Meter` — so OpenTelemetry, Prometheus, Application Insights, or plain
console logging all work without adapters. With no listener attached the instrumentation costs
almost nothing: the counters are no-ops and the activity source creates no `Activity` objects.

One name covers both traces and metrics:

```csharp
public const string SourceName = "Pulse.Mqtt";   // PulseMqttDiagnostics.SourceName
```

`PulseMqttDiagnostics.ActivitySource` is public if you want to start child spans of your own;
the `Meter` is internal but its instruments are collected by meter name.

## Metrics

All instruments are emitted from the client. The instrument name is the metric; the tags are the
dimensions you group and alert by.

| Instrument | Type | Tags | Recorded when |
| --- | --- | --- | --- |
| `pulse.mqtt.client.connect.attempts` | `Counter<long>` | `client.id` | Each connection attempt starts (including every retry) |
| `pulse.mqtt.client.connect.duration` | `Histogram<double>` (s) | `client.id`, `outcome` | A connection attempt finishes; `outcome` is `success` \| `error` |
| `pulse.mqtt.client.state.transitions` | `Counter<long>` | `client.id`, `state` | The connection state changes; `state` is the new state |
| `pulse.mqtt.client.messages.published` | `Counter<long>` | `client.id`, `disposition` | A publish completes; `disposition` is `Delivered` \| `Queued` \| `DroppedOffline` \| `InFlight` |
| `pulse.mqtt.client.publish.duration` | `Histogram<double>` (s) | `client.id`, `disposition` | A publish completes (same dispositions) |
| `pulse.mqtt.client.messages.received` | `Counter<long>` | `client.id` | An application message is delivered to the client |
| `pulse.mqtt.client.offline.queue.depth` | `ObservableGauge<long>` | `client.id` | Observed on collection: publishes currently waiting in the offline queue |
| `pulse.mqtt.client.offline.queue.dropped` | `ObservableCounter<long>` | `client.id` | Observed on collection: publishes the overflow policy has dropped |

`client.id` is the client's MQTT client identifier — keep it unique per live client, since the
observable gauges report one measurement per client tagged only by `client.id`, and two live
clients sharing an id collide on the same series. `state` takes the
[`ConnectionState`](/reference/connection-states) values; `disposition` takes the
[`PublishDisposition`](./publishing#outcomes--no-silent-loss) values (`Delivered`, `Queued`,
`DroppedOffline`, `InFlight`), plus two background-publish outcomes that share the
`messages.published` counter: `BirthFailed` (a [birth message](./presence) that failed under
`LogAndContinue`) and `DroppedTooLarge` (a queued publish dropped on reconnect because it exceeds
the broker's maximum packet size). The two histograms record seconds, so a percentile view
(p50/p95/p99) of connect and publish latency comes for free.

### Collect them with OpenTelemetry

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(PulseMqttDiagnostics.SourceName)   // "Pulse.Mqtt"
        .AddView("pulse.mqtt.*", new MetricStreamConfiguration { /* rename, drop tags, … */ })
        .AddPrometheusExporter());                    // or .AddOtlpExporter(), .AddConsoleExporter()
```

Expose the Prometheus endpoint:

```csharp
app.MapPrometheusScrapingEndpoint();   // /metrics
```

Scraped, the counters look like this (Prometheus normalizes dots to underscores and appends
`_total`):

```
pulse_mqtt_client_messages_published_total{client_id="svc-1",disposition="Delivered"} 14823
pulse_mqtt_client_messages_published_total{client_id="svc-1",disposition="Queued"} 7
pulse_mqtt_client_state_transitions_total{client_id="svc-1",state="Reconnecting"} 3
pulse_mqtt_client_connect_attempts_total{client_id="svc-1"} 11
```

## Traces

Three spans cover the connect, publish, and receive paths:

| Span | Kind | Display name | Tags |
| --- | --- | --- | --- |
| `connect` | `Client` | `connect` | `messaging.system=mqtt`, `client.id=<id>`, `pulse.mqtt.session_present=<bool>` |
| `publish` | `Producer` | `publish <topic>` | `messaging.system=mqtt`, `messaging.destination.name=<topic>`, `messaging.operation.type=send`, `pulse.mqtt.disposition=<disposition>` |
| `receive` | `Consumer` | `receive <topic>` | `messaging.system=mqtt`, `messaging.destination.name=<topic>`, `messaging.operation.type=process` |

The `connect` span wraps a single connection attempt (one per retry), so its duration matches the
`connect.duration` histogram and its status is set to error on a failed attempt. The `receive`
span wraps a [routed handler](./routing) invocation. The `messaging.*` tags follow the
OpenTelemetry messaging semantic conventions, so any trace UI that understands them renders the
spans correctly. The `publish` span participates in the ambient `Activity` context, so a publish
made inside an incoming request or consumer span is parented to it automatically.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(PulseMqttDiagnostics.SourceName)   // "Pulse.Mqtt"
        .AddAspNetCoreInstrumentation()               // parent spans, when relevant
        .AddOtlpExporter());                          // or Jaeger, Zipkin, console
```

### Trace context across the broker

To connect a producer's `publish` span to a consumer's `receive` span across processes, opt the
producer in to MQTT 5 trace-context propagation:

```csharp
var options = new ResilientMqttClientOptions
{
    Connect = connect,
    PropagateTraceContext = true,   // off by default
};
```

When enabled, the active span's `traceparent` (and `tracestate`) is written onto the user
properties of each outbound publish. On the other side the `receive` span **always** honors an
incoming `traceparent` — regardless of its own `PropagateTraceContext` setting — so the handler's
work, and any spans it starts, become children of the original `publish` span. The result is one
continuous distributed trace from producer through the broker to consumer. With propagation off
(the default) no `traceparent` is added and `receive` spans are local roots. Because extraction is
passive, the consumer also links correctly to non-Pulse producers that set a standard
`traceparent`.

This protocol-level path is MQTT 5 only. MQTT 3.1.1 has no user properties, so there is no
standard metadata slot for trace context. With MQTT 3.1.1, `PropagateTraceContext = true` is a
clean no-op on the wire: publishes still get local `publish` spans, but consumers cannot parent
on those spans unless your payload contract carries the context.

### Payload envelopes for MQTT 3.1.1

When you own both producer and consumer payload contracts, wrap the application payload in
`MqttTraceEnvelope<T>`. The helper captures the actual `publish` span context and serializes it
inside the payload:

```csharp
await client.PublishWithTraceEnvelopeAsync(
    "sensors/boiler-1/telemetry",
    new TelemetryReading("boiler-1", 21.5),
    MqttQualityOfService.AtLeastOnce,
    cancellationToken: token);
```

On the consumer side, register the matching envelope route. The route helper deserializes the
envelope and runs your handler under a consumer activity parented to the producer context when
one is present:

```csharp
using var route = client.RegisterTraceEnvelopeRoute<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    (reading, message, token) =>
    {
        ProcessReading(message.Values["deviceId"], reading);
        return ValueTask.CompletedTask;
    });
```

The envelope is an application payload shape, so serializers must know the closed generic type.
For source-generated JSON, add it to the context:

```csharp
[JsonSerializable(typeof(TelemetryReading))]
[JsonSerializable(typeof(MqttTraceEnvelope<TelemetryReading>))]
internal sealed partial class AppJsonContext : JsonSerializerContext;
```

Prefer MQTT 5 user properties when the broker and clients support MQTT 5. Use the payload
envelope only for MQTT 3.1.1 or for deployments that intentionally standardize on an envelope
payload. Avoid putting trace context in topics; it leaks metadata and creates high-cardinality
topic names.

## Logs

Pass a logger directly (`ResilientMqttClientOptions.Logger`) or let
[dependency injection](./dependency-injection#logging) create one per client
(`Pulse.Mqtt.Client.<name>`). Every message is a source-generated `LoggerMessage` — zero
allocation when the level is disabled.

| Event id | Level | Message template | Carries an exception |
| --- | --- | --- | --- |
| 1 `StateChanged` | Information | `MQTT client {ClientId} state {Previous} -> {Current} (attempt {Attempt})` | no |
| 2 `ConnectAttemptFailed` | Warning | `MQTT client {ClientId} connect attempt {Attempt} failed` | yes |
| 3 `ConnectionLost` | Information | `MQTT client {ClientId} lost its connection` | no |
| 4 `RouteHandlerFaulted` | Error | `MQTT route {Template} handler failed` | yes |

The structured properties (`ClientId`, `Previous`, `Current`, `Attempt`, `Template`) are
available to any structured sink (Seq, Elastic, Application Insights) for filtering and
correlation — for example, alert on `EventId = 2` with a rising `Attempt`.

Routing logs through OpenTelemetry as well:

```csharp
builder.Logging.AddOpenTelemetry(o =>
{
    o.IncludeScopes = true;
    o.AddOtlpExporter();
});
```

## Diagnostics snapshot

For polling-style diagnostics, `ResilientMqttClient.GetDiagnosticsSnapshot()` returns a
synchronous, in-memory view of the client. It does not inspect payloads, credentials, queued
messages, or topic lists.

```csharp
var snapshot = client.GetDiagnosticsSnapshot();

logger.LogInformation(
    "MQTT {State} attempt {Attempt}, queue {QueueDepth}, last reason {Reason}",
    snapshot.State,
    snapshot.Attempt,
    snapshot.OfflineQueueDepth,
    snapshot.LastReason);
```

The snapshot includes:

| Field | Meaning |
| --- | --- |
| `ClientId`, `State`, `Attempt`, `IsRunning`, `StateChangedAt` | Lifecycle position and when it last changed |
| `LastReason`, `LastReasonString`, `LastServerReference`, `LastError` | Last broker disconnect, rejected CONNECT, retry failure, or terminal fault details |
| `OfflineQueueDepth`, `OfflineQueueDroppedCount` | Queue counters, or `null` if a custom store cannot provide them during collection |
| `SubscriptionCount`, `PendingSubscribeCount`, `PendingUnsubscribeCount` | Durable subscription set and offline subscription deltas |

Successful `Connected` transitions clear stale fault details, so `LastError` describes the
current problem, not an old outage that has already recovered.

Use the snapshot when an operator-facing system needs a current value instead of an event
stream. Typical uses are periodic logs, dashboard gauges, and fault triage after an alert:

```csharp
var snapshot = client.GetDiagnosticsSnapshot();

logger.LogInformation(
    "MQTT client {ClientId} is {State} after {Attempt} attempts; queue depth {QueueDepth}",
    snapshot.ClientId,
    snapshot.State,
    snapshot.Attempt,
    snapshot.OfflineQueueDepth);

if (snapshot.LastError is { } error)
{
    logger.LogWarning(
        error,
        "MQTT last failure was {Reason}: {ReasonString}; server reference {ServerReference}",
        snapshot.LastReason,
        snapshot.LastReasonString,
        snapshot.LastServerReference);
}
```

For a small metrics bridge, prefer low-cardinality gauges. Queue counters are nullable because
custom stores can decline or fail counter reads without failing diagnostics collection:

```csharp
var meter = new Meter("MyApp.Mqtt");

meter.CreateObservableGauge(
    "app_mqtt_connected",
    () => client.GetDiagnosticsSnapshot().State == ConnectionState.Connected ? 1 : 0);

meter.CreateObservableGauge(
    "app_mqtt_offline_queue_depth",
    () => client.GetDiagnosticsSnapshot().OfflineQueueDepth ?? 0);
```

For reconnect and fault triage, start with `State`, `Attempt`, and `StateChangedAt`, then look
at `LastReason`, `LastReasonString`, `LastServerReference`, and `LastError`. A missing
`LastError` means the current state was not caused by a captured exception; a missing queue
counter means the store could not report it, not that the queue is empty.

## State as a stream or event

For in-process reactions — a UI badge, a circuit indicator, paging — you do not need the
metrics pipeline. Two surfaces expose state directly:

```csharp
// Pull: an async stream of transitions (see Lifecycle and state).
await foreach (var change in client.WatchState(token))
{
    if (change.Current == ConnectionState.Faulted)
        alerting.Page($"MQTT faulted: {change.Reason} {change.ReasonString}");
}

// Push: a plain event, if that fits better.
client.StateChanged += change =>
    metrics.SetGauge("mqtt_up", change.Current == ConnectionState.Connected ? 1 : 0);
```

`ConnectionStateChanged` carries `Previous`, `Current`, `Attempt`, `Reason`, `ReasonString`,
`ServerReference`, and `Error` (the triggering exception, when there is one). The original
constructor and deconstruction shape remain the four core fields, so existing handlers keep
compiling. The router exposes a matching `HandlerFaulted` event
(`Action<string, Exception>` — the route template and the exception) if you want to react to
handler faults beyond the `RouteHandlerFaulted` log.

## Alerting recipes

Built straight from the instruments above:

| Symptom | Signal |
| --- | --- |
| Broker link flapping | `rate(pulse_mqtt_client_messages_published_total{disposition="Queued"}[5m]) > 0` |
| Broker down or rejecting | `rate(pulse_mqtt_client_connect_attempts_total[5m])` rising with no `state="Connected"` transition |
| Client gave up (terminal) | any `pulse_mqtt_client_state_transitions_total{state="Faulted"}` — page immediately; it stopped retrying for a reason |
| Offline drops | `increase(pulse_mqtt_client_offline_queue_dropped_total[5m]) > 0` — the overflow policy is shedding load |
| Offline backlog growing | `pulse_mqtt_client_offline_queue_depth` trending up while disconnected |
| Consumer stalled | `messages.received` flat while the broker is known to be publishing |

## Health checks

For orchestrator and load-balancer probes, map the connection state to a standard health
result — see [Health checks](./health-checks).
