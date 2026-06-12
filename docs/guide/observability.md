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

Four counters, all `Counter<long>`, all emitted from the client. The instrument name is the
metric; the tags are the dimensions you group and alert by.

| Instrument | Type | Tags | Incremented when |
| --- | --- | --- | --- |
| `pulse.mqtt.client.connect.attempts` | `Counter<long>` | `client.id` | Each connection attempt starts (including every retry) |
| `pulse.mqtt.client.state.transitions` | `Counter<long>` | `client.id`, `state` | The connection state changes; `state` is the new state |
| `pulse.mqtt.client.messages.published` | `Counter<long>` | `client.id`, `disposition` | A publish completes; `disposition` is `Delivered` \| `Queued` \| `DroppedOffline` |
| `pulse.mqtt.client.messages.received` | `Counter<long>` | `client.id` | An application message is delivered to the client |

`client.id` is the client's MQTT client identifier. `state` takes the
[`ConnectionState`](/reference/connection-states) values; `disposition` takes the
[`PublishDisposition`](./publishing#outcomes--no-silent-loss) values.

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

One span, from the publish path:

| Span | Kind | Display name | Tags |
| --- | --- | --- | --- |
| `publish` | `Producer` | `publish <topic>` | `messaging.system=mqtt`, `messaging.destination.name=<topic>`, `messaging.operation.type=send`, `pulse.mqtt.disposition=<disposition>` |

The three `messaging.*` tags follow the OpenTelemetry messaging semantic conventions, so any
trace UI that understands them renders the span correctly. The span participates in the ambient
`Activity` context, so a publish made inside an incoming request or consumer span is parented to
it automatically — you get end-to-end traces across the broker boundary for free.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(PulseMqttDiagnostics.SourceName)   // "Pulse.Mqtt"
        .AddAspNetCoreInstrumentation()               // parent spans, when relevant
        .AddOtlpExporter());                          // or Jaeger, Zipkin, console
```

Receive and connect paths are not yet spanned — use the `messages.received` /
`connect.attempts` metrics and the logs below for those.

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

## State as a stream or event

For in-process reactions — a UI badge, a circuit indicator, paging — you do not need the
metrics pipeline. Two surfaces expose state directly:

```csharp
// Pull: an async stream of transitions (see Lifecycle and state).
await foreach (var change in client.WatchState(token))
{
    if (change.Current == ConnectionState.Faulted)
        alerting.Page($"MQTT faulted: {change.Reason}");
}

// Push: a plain event, if that fits better.
client.StateChanged += change =>
    metrics.SetGauge("mqtt_up", change.Current == ConnectionState.Connected ? 1 : 0);
```

`ConnectionStateChanged` carries `Previous`, `Current`, `Attempt`, and `Error` (the triggering
exception, when there is one). The router exposes a matching `HandlerFaulted` event
(`Action<string, Exception>` — the route template and the exception) if you want to react to
handler faults beyond the `RouteHandlerFaulted` log.

## Alerting recipes

Built straight from the instruments above:

| Symptom | Signal |
| --- | --- |
| Broker link flapping | `rate(pulse_mqtt_client_messages_published_total{disposition="Queued"}[5m]) > 0` |
| Broker down or rejecting | `rate(pulse_mqtt_client_connect_attempts_total[5m])` rising with no `state="Connected"` transition |
| Client gave up (terminal) | any `pulse_mqtt_client_state_transitions_total{state="Faulted"}` — page immediately; it stopped retrying for a reason |
| Offline drops | `IMessageStore.DroppedCount` climbing (expose it as a gauge from your own meter if you need it on a dashboard) |
| Consumer stalled | `messages.received` flat while the broker is known to be publishing |

## Health checks

For orchestrator and load-balancer probes, map the connection state to a standard health
result — see [Health checks](./health-checks).
