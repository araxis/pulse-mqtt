# Health checks

Pulse maps the client's connection state to a standard `Microsoft.Extensions.Diagnostics.HealthChecks`
result, so orchestrators (Kubernetes, container platforms) and load balancers can probe it the
same way they probe everything else.

## Register

```csharp
builder.Services
    .AddPulseMqttClient("devices", configure)
    .AddHealthCheck();
```

That registers a check named **`pulse-mqtt-<name>`** — for the client above,
`pulse-mqtt-devices`. Register the ASP.NET Core endpoint as usual:

```csharp
app.MapHealthChecks("/health");
```

## The state mapping

The check reads `ResilientMqttClient.GetDiagnosticsSnapshot()` and maps the snapshot state
exactly:

| Connection state | Health status | Description |
| --- | --- | --- |
| `Connected` | **Healthy** | `Connected (attempt <attempt>).` |
| `Connecting`, `Reconnecting`, `WaitingRetry` | **Degraded** | `The connection is being established (<state>, attempt <attempt>).` |
| `Disconnected`, `Faulted`, `Stopped` | **Unhealthy** | `The client is <state>.`, or `The client is <state> (<reason>).` when a reason is known |

`Degraded` is the deliberate middle ground: the client is doing its job (reconnecting), so a
transient blip does not flap your readiness probe, but the dashboard still shows it is not fully
up. `Faulted` is `Unhealthy` because the client has stopped retrying — it needs intervention.

## Result data

The built-in check attaches snapshot data for dashboards and probe logs:

| Key | Meaning |
| --- | --- |
| `client.id`, `state`, `attempt`, `is.running`, `state.changed_at` | Lifecycle position |
| `subscription.count`, `pending.subscribe.count`, `pending.unsubscribe.count` | Subscription bookkeeping |
| `reason`, `reason.string`, `server.reference` | Last broker or connect reason details, when available |
| `error.type`, `error.message` | Last exception details, when available |
| `offline.queue.depth`, `offline.queue.dropped` | Queue counters, when the store can report them |

## Separating liveness from readiness

A resilient client that is reconnecting is *alive* but not *ready*. Tag the check and split the
endpoints so a reconnect doesn't restart your pod:

```csharp
builder.Services
    .AddPulseMqttClient("devices", configure)
    .AddHealthCheck();   // tag it via the registration below if you need finer control

// Readiness: only Healthy passes — traffic waits for a live broker link.
app.MapHealthChecks("/ready", new HealthCheckOptions
{
    Predicate = reg => reg.Name == "pulse-mqtt-devices",
});

// Liveness: Degraded still counts as alive, so reconnects don't trigger a restart.
app.MapHealthChecks("/live", new HealthCheckOptions
{
    Predicate = reg => reg.Name == "pulse-mqtt-devices",
    ResultStatusCodes =
    {
        [HealthStatus.Healthy] = StatusCodes.Status200OK,
        [HealthStatus.Degraded] = StatusCodes.Status200OK,   // alive while reconnecting
        [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
    },
});
```

## Multiple clients

Each `AddHealthCheck()` registers an independent check named after its client, so a dashboard
shows them separately and a probe can target one:

```csharp
builder.Services.AddPulseMqttClient("telemetry", ConfigureTelemetry).AddHealthCheck();
builder.Services.AddPulseMqttClient("commands", ConfigureCommands).AddHealthCheck();
// → pulse-mqtt-telemetry, pulse-mqtt-commands
```

## Without dependency injection

The check is a plain `IHealthCheck` over a client — construct it directly when you manage your
own health pipeline:

```csharp
var check = new PulseMqttHealthCheck(client);
HealthCheckResult result = await check.CheckHealthAsync(
    new HealthCheckContext { Registration = registration }, ct);
```

## A custom health check

If you want a different definition of healthy — say, also requiring the offline queue to be
below a threshold — write your own over the same snapshot:

```csharp
public sealed class MqttBacklogHealthCheck(IPulseMqttClientFactory clients) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct)
    {
        var client = clients.GetClient("devices");
        var snapshot = client.GetDiagnosticsSnapshot();

        if (snapshot.State != ConnectionState.Connected)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy($"MQTT is {snapshot.State}."));
        }

        if (snapshot.OfflineQueueDepth is > 500)
        {
            return Task.FromResult(
                HealthCheckResult.Degraded($"Offline backlog {snapshot.OfflineQueueDepth}."));
        }

        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
```

```csharp
builder.Services.AddHealthChecks().AddCheck<MqttBacklogHealthCheck>("mqtt-backlog");
```

The diagnostics snapshot and state stream behind the built-in check are also available directly
for non-HTTP reactions — see [Observability](./observability#diagnostics-snapshot).
