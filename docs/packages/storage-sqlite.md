# SQLite storage package

Package: `Pulse.Mqtt.Storage.Sqlite`

Use this package when a client needs durable session state and an offline queue backed by a
single relational file store technology.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.Sqlite
```

## What it provides

| Type | Contract | Stores |
| --- | --- | --- |
| `SqliteSessionStore` | `ISessionStore` | Durable subscriptions and in-flight QoS state. |
| `SqliteMessageStore` | `IMessageStore` | Offline outbound publishes in FIFO order. |
| `SqliteStorageException` | Exception | Database open, schema, lock, and malformed stored-packet failures. |

The stores use the same contracts as the in-memory defaults. Reconnect, publish, subscribe, and
flush behavior do not change when the storage implementation changes.

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Storage.Sqlite;

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseSessionStore(_ => new SqliteSessionStore("devices-session.db"))
    .UseMessageStore(_ => new SqliteMessageStore(
        "devices-queue.db",
        new OfflineQueueOptions
        {
            Capacity = 10_000,
            Overflow = OverflowPolicy.Block,
            IncludeQos0 = false,
        }));
```

Use separate files for session state and queued messages when operational ownership differs. Use
a connection string instead of a plain path when you need provider-specific options.

## Configure directly

```csharp
var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket
    {
        ClientId = "device-worker",
        CleanStart = false,
        SessionExpiryInterval = 300,
    },
    SessionStore = new SqliteSessionStore("devices-session.db"),
    MessageStore = new SqliteMessageStore(
        "devices-queue.db",
        new OfflineQueueOptions { Capacity = 10_000 }),
};
```

Set `CleanStart = false` and a non-zero session expiry when the broker should preserve session
state across reconnects. The local session store preserves the client's view of subscriptions and
in-flight QoS state across process restarts.

## Offline queue behavior

`SqliteMessageStore` follows the same `IMessageStore` contract as the in-memory queue:

- `EnqueueAsync` writes a queued publish.
- `PeekAsync` reads the oldest publish without removing it.
- `RemoveHeadAsync` removes the publish after a successful flush.
- `DroppedCount` reports overflow drops.

The peek-then-remove flow favors at-least-once recovery: if the process stops after send and
before remove, the publish can be retried after restart.

## Operational notes

- Plain paths are accepted and normalized to SQLite connection strings.
- Store instances are disposable; dispose them when they are not owned by the client/service
  provider lifetime.
- A locked, missing, unreadable, truncated, or malformed database fails explicitly with
  `SqliteStorageException`.
- The package is intended for one process owning the store files. Use a server database or custom
  store when multiple processes must coordinate writes.

## Related docs

- [Resilience durable storage](/guide/resilience#durable-storage)
- [Extending stores](/guide/extending#session-store)
- [Package add-ons](/guide/package-add-ons)
