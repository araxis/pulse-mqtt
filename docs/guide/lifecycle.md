# Lifecycle and state

## Connect and disconnect

```csharp
await client.ConnectAsync(token);   // returns quickly; connection happens in the background
await client.DisconnectAsync(token); // disconnects and stops reconnecting
await client.ConnectAsync(token);   // restart any time — including after a fault
```

- `ConnectAsync` launches the supervisor and returns once the first connection attempt is under
  way — watch `State` or `WatchState` for progress. Calling it while already running throws.
- `DisconnectAsync` is **idempotent**: disconnecting a stopped (or never-started) client is a no-op.
- Disconnect → connect round trips are fully supported, and `ConnectAsync` is also the recovery path
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
    options.ConnectWithHost = false;     // the host will not auto-connect this client
});
```

```csharp
var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");
await client.ConnectAsync(token);        // on a feature flag, a UI toggle, a schedule…
await client.DisconnectAsync(token);
await client.ConnectAsync(token);
```

Host shutdown still stops a running client in both modes — clean teardown is never your
problem.

## The states

| State | Meaning |
| --- | --- |
| `Disconnected` | Initial; before the first `ConnectAsync` |
| `Connecting` | First connection attempt in progress |
| `Connected` | A session is live |
| `Reconnecting` | The connection dropped; restoring |
| `WaitingRetry` | Backing off between attempts |
| `Faulted` | Terminal failure; sticky until an explicit `ConnectAsync` |
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
            alerting.Page($"MQTT client faulted: {change.Reason}");
        }
    }
});
```

Each watcher gets an independent bounded view (oldest entries drop if a watcher lags — it
never blocks the client). Subscribe before the transitions you care about; there is no replay.

`ConnectionStateChanged` also carries `Reason`, `ReasonString`, `ServerReference`, and `Error`
when a broker disconnect, rejected CONNECT, retry failure, or terminal fault has details to
report.

When state changes feed a Dataflow pipeline, add `Pulse.Mqtt.Dataflow` and use
`ToStateSourceBlock(...)`. The source is bounded and can also be exposed as an observable through
`DataflowBlock.AsObservable(source)`.

## Reading diagnostics

For a synchronous point-in-time view, call `GetDiagnosticsSnapshot()`:

```csharp
var snapshot = client.GetDiagnosticsSnapshot();

logger.LogInformation(
    "MQTT {State} attempt {Attempt}, subscriptions {SubscriptionCount}, queued {Queued}",
    snapshot.State,
    snapshot.Attempt,
    snapshot.SubscriptionCount,
    snapshot.OfflineQueueDepth);
```

The snapshot includes the current state, when it changed, the last reason/error details, offline
queue counters, and subscription bookkeeping. Queue counters are nullable so a custom store can
fail diagnostics collection without failing the caller. For structured logging, metrics export,
and health-check result data, see [Observability](./observability#diagnostics-snapshot) and
[Health checks](./health-checks#result-data).

## Waiting for connected

`ConnectAsync` deliberately does not block until connected — brokers can be down, and resilience
means living with that. When code genuinely needs a live connection first:

```csharp
await client.ConnectAsync(token);
await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(10), token);
```

Most code should not wait: publishes [queue while offline](./resilience#the-offline-queue) and
subscriptions apply on connect, so working through the client is safe in every state.
For custom readiness rules, use `WatchState` directly and react to the transitions that matter
to your application.

## Health checks

```csharp
services.AddPulseMqttClient("devices", configure)
    .AddHealthCheck();
```

Maps the connection state to health: `Connected` → healthy, transient states → degraded,
`Faulted`/`Stopped`/`Disconnected` → unhealthy. See
[Dependency injection](./dependency-injection#health-checks).
