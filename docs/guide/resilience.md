# Resilience

The resilient client is a **supervisor** around per-connection sessions. Your handles — the
client, its routes, the message stream, the publish API — live across any number of underlying
connections.

## The reconnect cycle

```
ConnectAsync
   │
   ▼
Connecting ──connect──▶ Connected ──connection lost──▶ Reconnecting
   │  ▲                     │                              │
   │  └──── WaitingRetry ◀──┴──────────────────────────────┘
   │            (backoff between attempts)
   ▼
Faulted (terminal failure — sticky)        Stopped (DisconnectAsync)
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

## Broker-initiated disconnects

A broker that sends DISCONNECT gets an orderly close, not a guessing game. The reason code,
reason string, and server reference are surfaced everywhere they matter: in-flight operations
fail with `MqttServerDisconnectedException` carrying all three, the
`ConnectionStateChanged` transition carries the code, reason string, server reference, and
error, the lifecycle's down-context carries the details (including `ServerReference` for
redirect-aware deployments), `GetDiagnosticsSnapshot()` exposes the last details for polling,
and the `ServerDisconnected` log event records it.

What happens next depends on the reason, through the same `IReconnectDecision`:

- **Transient reasons** — `ServerShuttingDown`, `ServerBusy`, keep-alive timeouts — reconnect
  with the normal backoff.
- **Terminal reasons** — `NotAuthorized`, `Banned`, `ServerMoved`, `UseAnotherServer`, and
  notably `SessionTakenOver` — fault sticky instead. `SessionTakenOver` is terminal on
  purpose: another connection owns the session now, and auto-reconnecting would steal it back
  in an endless takeover war.

Swap the decision to change the classification, or — for redirects specifically — turn on the
built-in following below.

### Following server redirects

Clusters rebalance by redirecting clients: a DISCONNECT (or CONNACK rejection) with
`UseAnotherServer` / `ServerMoved` and a `Server Reference` naming the next broker. By default
that faults sticky, surfacing the reference for the operator. Opt in to have the client follow
it instead:

```csharp
new ResilientMqttClientOptions
{
    FollowServerRedirects = true,
    // MaxServerRedirects = 8 — rapid hops allowed before the next redirect is terminal.
}
```

The client re-targets its transport at the referenced `host[:port]` (space-delimited lists use
the first entry; bracketed IPv6 works) and reconnects there — session store, offline queue, and
subscriptions all carry over, exactly like any other reconnect. The built-in TCP, WebSocket,
and QUIC factories support re-targeting; a custom factory opts in by implementing
`IRedirectableTransportFactory`.

The hop bound exists for the misconfigured case of brokers redirecting to each other in a loop:
rapid hops count against `MaxServerRedirects` even when each hop briefly connects, and the
budget renews once the chain has been quiet for a minute — so occasional rebalancing over days
of uptime is never punished.

## Sticky faults

Some failures should **not** be retried: `NotAuthorized`, `BadUserNameOrPassword`, a banned
client identifier, a session takeover. The `IReconnectDecision` classifies each failure;
terminal ones move the client to `Faulted`, where it stays — visibly — instead of hammering
the broker forever.

Recovery is explicit:

```csharp
if (client.State == ConnectionState.Faulted)
{
    await RotateCredentialsAsync();
    await client.ConnectAsync(token);   // restart after the cause is fixed
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

### Message expiry while queued

A publish with an MQTT 5 `MessageExpiryInterval` keeps expiring while it waits in the queue.
On flush, a message that outlived its expiry is dropped instead of delivered stale — logged,
and counted under the `DroppedExpired` metric disposition — and a surviving message goes out
with the time it waited subtracted from its remaining interval, exactly as a broker would
forward it. Messages without an expiry are unaffected. Custom `IMessageStore` implementations
opt in by overriding `EnqueueAsync(packet, enqueuedAt, ...)` and `PeekQueuedAsync`; without the
override, messages flush as before, with no expiry accounting.

## In-flight redelivery on session resume

With a **persistent session** — `CleanStart = false` and a broker that preserves the session —
Pulse honors the MQTT 5 requirement to retransmit unfinished QoS 1/2 work after a reconnect,
so a publish interrupted mid-exchange is never lost:

- An unacknowledged QoS 1/2 PUBLISH is recorded the moment it goes to the wire. If the
  connection drops before its acknowledgement, `PublishAsync` returns
  `PublishDisposition.InFlight` — the message is held, not lost.
- On reconnect, if the broker reports the session is present, the held exchanges **redeliver in
  their original order, before the offline queue flushes**. A PUBLISH still awaiting its PUBACK
  or PUBREC re-sends with the **DUP** flag and its original packet identifier; an exchange that
  already received its PUBREC re-sends only the **PUBREL**.
- Inbound QoS 2 duplicate-suppression state is restored too after Pulse has accepted the
  delivery with PUBREC, so a message the broker redelivers after the reconnect is acknowledged
  but **not delivered to your handlers twice**. If you opt into manual acknowledgement route
  delivery and the connection drops before your code calls `AcknowledgeAsync`, Pulse has not sent
  PUBREC yet; the broker redelivery can reach your manual handler or stream again.
- If the broker reports a **fresh** session (it did not preserve the old one), the in-flight
  state is discarded per the specification.

The tracked state lives behind the [`ISessionStore`](./extending#custom-session-store) swap
point: the in-memory default keeps it for the process (covering reconnects within a run), and a
durable store carries it across process restarts. Clean-start clients (the default) skip the
tracking entirely, so the hot publish path keeps its zero-allocation cost.

```csharp
new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket
    {
        ClientId = "device-7",
        CleanStart = false,            // resume an existing session
        SessionExpiryInterval = 300,   // ask the broker to keep it for 5 minutes
    },
};
```

`SessionExpiryInterval` is MQTT 5-only. With MQTT 3.1.1, `CleanStart = false` still requests a
persistent session, but there is no portable expiry timer in the protocol; the broker's policy
decides how long to keep it.

## Durable storage

The defaults keep everything for the lifetime of the process. To carry the subscription set, the
offline queue, and the in-flight QoS state across a **process restart**, add a durable storage
package and point both stores at the provider's backing store.

SQLite:

```csharp
using Pulse.Mqtt.Storage.Sqlite;

var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "device-7", CleanStart = false, SessionExpiryInterval = 300 },
    SessionStore = new SqliteSessionStore("device-7-session.db"),
    MessageStore = new SqliteMessageStore("device-7-queue.db", new OfflineQueueOptions { Capacity = 1024 }),
};
```

LiteDB:

```csharp
using Pulse.Mqtt.Storage.LiteDB;

var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "device-7", CleanStart = false, SessionExpiryInterval = 300 },
    SessionStore = new LiteDbSessionStore("device-7-session.db"),
    MessageStore = new LiteDbMessageStore("device-7-queue.db", new OfflineQueueOptions { Capacity = 1024 }),
};
```

SQL Server:

```csharp
using Pulse.Mqtt.Storage.SqlServer;

var storage = new SqlServerStorageOptions { SchemaName = "mqtt", TablePrefix = "Device7" };

var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "device-7", CleanStart = false, SessionExpiryInterval = 300 },
    SessionStore = new SqlServerSessionStore(connectionString, storage),
    MessageStore = new SqlServerMessageStore(connectionString, new OfflineQueueOptions { Capacity = 1024 }, storage),
};
```

SQLite and LiteDB accept a plain file path or the provider's connection string and create the
database on first use. SQL Server expects the database to already exist, then creates the configured
schema and tables when the login has permission. The message store preserves FIFO order and the same
overflow policy as the in-memory default; because the flush loop peeks then removes the head, a crash
between sending and removing re-sends the message rather than losing it. A missing, locked,
unavailable, or corrupt store fails fast with a provider-specific storage exception instead of
silently starting empty.

## Sessions and re-subscription

The durable subscription set lives in an `ISessionStore` (in-memory by default). On
connection-up, the default `IConnectionLifecycle` re-subscribes the stored set when the broker
reports a fresh session (`CleanStart = false` with a preserved session skips the work). Swap
the lifecycle to add cache warming, announcements, or custom ordering.

## Watching it happen

```csharp
await foreach (var change in client.WatchState(token))
{
    // change.Previous, change.Current, change.Attempt, change.Reason,
    // change.ReasonString, change.ServerReference, change.Error
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
