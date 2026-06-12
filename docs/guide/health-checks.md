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

The check reads `ResilientMqttClient.State` and maps it exactly:

| Connection state | Health status | Description |
| --- | --- | --- |
| `Connected` | **Healthy** | `Connected.` |
| `Connecting`, `Reconnecting`, `WaitingRetry` | **Degraded** | `The connection is being established (<state>).` |
| `Disconnected`, `Faulted`, `Stopped` | **Unhealthy** | `The client is <state>.` |

`Degraded` is the deliberate middle ground: the client is doing its job (reconnecting), so a
transient blip does not flap your readiness probe, but the dashboard still shows it is not fully
up. `Faulted` is `Unhealthy` because the client has stopped retrying — it needs intervention.

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
below a threshold — write your own over the same surfaces:

```csharp
public sealed class MqttBacklogHealthCheck(
    IPulseMqttClientFactory clients,
    IMessageStore offlineQueue) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct)
    {
        var client = clients.GetClient("devices");
        if (client.State != ConnectionState.Connected)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy($"MQTT is {client.State}."));
        }

        // Count and DroppedCount are part of the IMessageStore contract.
        if (offlineQueue.Count > 500)
        {
            return Task.FromResult(HealthCheckResult.Degraded($"Offline backlog {offlineQueue.Count}."));
        }

        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
```

```csharp
builder.Services.AddHealthChecks().AddCheck<MqttBacklogHealthCheck>("mqtt-backlog");
```

The state stream behind the built-in check is also available directly for non-HTTP reactions —
see [Observability](./observability#state-as-a-stream-or-event).
