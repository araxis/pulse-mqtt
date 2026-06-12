# Lifecycle and state

## Start and stop

```csharp
await client.StartAsync(token);   // returns quickly; connection happens in the background
await client.StopAsync(token);    // disconnects and stops reconnecting
await client.StartAsync(token);   // restart any time — including after a fault
```

- `StartAsync` launches the supervisor and returns once the first connection attempt is under
  way — watch `State` or `WatchState` for progress. Calling it while already running throws.
- `StopAsync` is **idempotent**: stopping a stopped (or never-started) client is a no-op.
- Stop → start round trips are fully supported, and `StartAsync` is also the recovery path
  from the `Faulted` state.
- `DisposeAsync` stops the client and releases everything; a disposed client cannot restart.

## Host-managed or manual

With dependency injection, the default ties the client to the host: started on host startup,
stopped on shutdown. To control it yourself:

```csharp
services.AddPulseMqttClient("devices", options =>
{
    options.Host = "broker.example.com";
    options.ClientId = "my-service";
    options.StartWithHost = false;     // the host will not auto-start this client
});
```

```csharp
var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");
await client.StartAsync(token);        // on a feature flag, a UI toggle, a schedule…
await client.StopAsync(token);
await client.StartAsync(token);
```

Host shutdown still stops a running client in both modes — clean teardown is never your
problem.

## The states

| State | Meaning |
| --- | --- |
| `Disconnected` | Initial; before the first `StartAsync` |
| `Connecting` | First connection attempt in progress |
| `Connected` | A session is live |
| `Reconnecting` | The connection dropped; restoring |
| `WaitingRetry` | Backing off between attempts |
| `Faulted` | Terminal failure; sticky until an explicit `StartAsync` |
| `Stopped` | Stopped at the caller's request |

## Watching transitions

`State` is a snapshot; `WatchState` is the stream:

```csharp
_ = Task.Run(async () =>
{
    await foreach (var change in client.WatchState(stoppingToken))
    {
        logger.LogInformation("MQTT {Previous} -> {Current} (attempt {Attempt})",
            change.Previous, change.Current, change.Attempt);

        if (change.Current == ConnectionState.Faulted)
        {
            alerting.Page("MQTT client faulted", change.Error);
        }
    }
});
```

Each watcher gets an independent bounded view (oldest entries drop if a watcher lags — it
never blocks the client). Subscribe before the transitions you care about; there is no replay.

## Waiting for connected

`StartAsync` deliberately does not block until connected — brokers can be down, and resilience
means living with that. When code genuinely needs a live connection first:

```csharp
await client.StartAsync(token);
await foreach (var change in client.WatchState(token))
{
    if (change.Current == ConnectionState.Connected) break;
    if (change.Current == ConnectionState.Faulted) throw new InvalidOperationException("MQTT faulted", change.Error);
}
```

Most code should not wait: publishes [queue while offline](./resilience#the-offline-queue) and
subscriptions apply on connect, so working through the client is safe in every state.

## Health checks

```csharp
services.AddPulseMqttClient("devices", configure)
    .AddHealthCheck();
```

Maps the connection state to health: `Connected` → healthy, transient states → degraded,
`Faulted`/`Stopped`/`Disconnected` → unhealthy. See
[Dependency injection](./dependency-injection#health-checks).
