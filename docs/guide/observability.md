# Observability

Everything important is observable through the standard .NET primitives — `ILogger`,
`ActivitySource`, `Meter` — so OpenTelemetry, Prometheus, Application Insights, or plain
console logging all work without adapters. With no listener attached, the instrumentation
costs almost nothing.

The source name for both traces and metrics is **`Pulse.Mqtt`**
(`PulseMqttDiagnostics.SourceName`).

## Logs

Pass a logger directly (`ResilientMqttClientOptions.Logger`) or let
[dependency injection](./dependency-injection#logging) create one per client
(`Pulse.Mqtt.Client.<name>`). All messages are source-generated `LoggerMessage` definitions —
zero allocation when the level is disabled.

| Event | Level | When |
| --- | --- | --- |
| `StateChanged` | Information | Every connection state transition |
| `ConnectAttemptFailed` | Warning | A connection attempt failed (with attempt number and error) |
| `ConnectionLost` | Information | A live connection dropped |
| `RouteHandlerFaulted` | Error | A route handler threw; the route is isolated |

## Traces

Spans from the `Pulse.Mqtt` activity source:

| Span | Kind | Highlights |
| --- | --- | --- |
| `connect` | Client | Connection attempts, with the attempt number |
| `publish` | Producer | `publish <topic>`, tagged with topic, QoS, and the outcome disposition |

```csharp
// OpenTelemetry:
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource(PulseMqttDiagnostics.SourceName));
```

## Metrics

| Instrument | Type | Meaning |
| --- | --- | --- |
| `pulse.mqtt.client.connect.attempts` | Counter | Connection attempts, including retries |
| `pulse.mqtt.client.state.transitions` | Counter | State transitions, tagged with the states |
| `pulse.mqtt.client.messages.published` | Counter | Publishes, tagged by disposition (delivered, queued, dropped) |
| `pulse.mqtt.client.messages.received` | Counter | Application messages received |

All tagged with `client.id`.

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter(PulseMqttDiagnostics.SourceName));
```

Useful alerts that fall straight out of these:

- `messages.published{disposition="queued"}` rising — the broker link is flapping.
- `connect.attempts` rising without `state.transitions` to `Connected` — the broker is down
  or rejecting.
- `state.transitions` into `Faulted` — page someone; the client stopped retrying for a
  reason.

## State as a stream

For in-process reactions (circuit indicators, UI badges, alerting), skip polling and consume
[`WatchState`](./lifecycle#watching-transitions).

## Health checks

The DI package maps connection state to standard health-check results — see
[Dependency injection](./dependency-injection#health-checks).
