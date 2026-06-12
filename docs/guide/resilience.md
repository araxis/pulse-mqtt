# Resilience

The resilient client is a **supervisor** around per-connection sessions. Your handles — the
client, its routes, the message stream, the publish API — live across any number of underlying
connections.

## The reconnect cycle

```
StartAsync
   │
   ▼
Connecting ──connect──▶ Connected ──connection lost──▶ Reconnecting
   │  ▲                     │                              │
   │  └──── WaitingRetry ◀──┴──────────────────────────────┘
   │            (backoff between attempts)
   ▼
Faulted (terminal failure — sticky)        Stopped (StopAsync)
```

On every successful (re)connection, **strict order**:

1. The CONNECT handshake completes.
2. The lifecycle hook runs — by default it **re-subscribes the durable subscription set**
   when the broker did not preserve the session.
3. The **offline queue flushes**, oldest first.
4. The state becomes `Connected` and live traffic resumes.

Re-subscription always precedes the flush, so a queued publish can never arrive at a broker
that has not restored its subscriptions.

## Backoff

The default strategy is exponential backoff with **full jitter**, capped, retrying forever:

```csharp
new ResilientMqttClientOptions
{
    Connect = connect,
    Backoff = new BackoffOptions
    {
        BaseDelay = TimeSpan.FromMilliseconds(500),  // doubles each attempt
        MaxDelay = TimeSpan.FromSeconds(30),         // growth cap
        MaxAttempts = null,                          // null = retry indefinitely
    },
};
```

Want Polly instead? The whole loop is a swap point:

```csharp
.UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline))
```

See [Extending](./extending#custom-reconnect-strategy).

## Sticky faults

Some failures should **not** be retried: `NotAuthorized`, `BadUserNameOrPassword`, a banned
client identifier. The `IReconnectDecision` classifies each failure; terminal ones move the
client to `Faulted`, where it stays — visibly — instead of hammering the broker forever.

Recovery is explicit:

```csharp
if (client.State == ConnectionState.Faulted)
{
    await RotateCredentialsAsync();
    await client.StartAsync(token);   // restart after the cause is fixed
}
```

Token-based systems where `NotAuthorized` is transient can swap the decision — see
[Extending](./extending#custom-reconnect-decision).

## The offline queue

Publishes made while disconnected are queued and flushed on reconnect. Everything about it is
bounded and explicit (`ResilientMqttClientOptions.OfflineQueue`):

| Setting | Default | Meaning |
| --- | --- | --- |
| `Capacity` | 1024 | Maximum queued publishes |
| `Overflow` | `Block` | `Block`, `DropOldest`, `DropNewest`, or `Reject` |
| `IncludeQos0` | `false` | Queue QoS 0 too, instead of dropping (counted, never silent) |
| `PublishWaitTimeout` | `null` | How long a `Block`ed publish waits before `OfflineQueueFullException`; `null` waits indefinitely |

Choosing an overflow policy:

- **`Block`** — backpressure to the publisher; nothing is lost while the process lives.
- **`DropOldest`** — latest-wins telemetry.
- **`DropNewest`** — preserve the backlog, shed new load.
- **`Reject`** — fail fast with `OfflineQueueFullException` and let the caller decide.

The queue is in-memory by default; a durable store that survives restarts is one interface
away — [Extending](./extending#custom-message-store).

## Sessions and re-subscription

The durable subscription set lives in an `ISessionStore` (in-memory by default). On
connection-up, the default `IConnectionLifecycle` re-subscribes the stored set when the broker
reports a fresh session (`CleanStart = false` with a preserved session skips the work). Swap
the lifecycle to add cache warming, announcements, or custom ordering.

## Watching it happen

```csharp
await foreach (var change in client.WatchState(token))
{
    // change.Previous, change.Current, change.Attempt, change.Error
}
```

Plus structured logs, `pulse.mqtt.client.connect.attempts` and
`pulse.mqtt.client.state.transitions` metrics, and connect spans — see
[Observability](./observability).

## What resilience does not hide

- A publish during an outage **tells you** it was queued (`PublishOutcome.Queued`) — see
  [Publishing](./publishing#outcomes--no-silent-loss).
- A terminal failure **stops** the client visibly rather than retrying into a wall.
- Queue overflow follows **your** policy, including failing fast.
