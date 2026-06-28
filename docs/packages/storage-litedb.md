# LiteDB storage package

Package: `Pulse.Mqtt.Storage.LiteDB`

Use this package when a client needs durable session state and an offline queue backed by an
embedded document database.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.LiteDB
```

## What it provides

| Type | Contract | Stores |
| --- | --- | --- |
| `LiteDbSessionStore` | `ISessionStore` | Durable subscriptions and in-flight QoS state. |
| `LiteDbMessageStore` | `IMessageStore` | Offline outbound publishes in FIFO order. |
| `LiteDbStorageException` | Exception | Database open, schema, lock, and malformed stored-packet failures. |

The package is API-compatible with the SQLite storage package at the client boundary. Pick one
durable store package per client unless there is a deliberate reason to split session and queue
storage across technologies.

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Storage.LiteDB;

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseSessionStore(_ => new LiteDbSessionStore("devices-session.db"))
    .UseMessageStore(_ => new LiteDbMessageStore(
        "devices-queue.db",
        new OfflineQueueOptions
        {
            Capacity = 10_000,
            Overflow = OverflowPolicy.Block,
            IncludeQos0 = false,
        }));
```

LiteDB connection strings are accepted. A plain file path is also valid.

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
    SessionStore = new LiteDbSessionStore("devices-session.db"),
    MessageStore = new LiteDbMessageStore(
        "devices-queue.db",
        new OfflineQueueOptions { Capacity = 10_000 }),
};
```

## Offline queue behavior

`LiteDbMessageStore` preserves FIFO order and the normal offline queue overflow policy:

- `Block` waits for space, bounded by `PublishWaitTimeout` when set.
- `DropOldest` removes the oldest queued publish before inserting the new one.
- `DropNewest` drops the new publish and increments `DroppedCount`.
- `Reject` throws `OfflineQueueFullException`.

The flush loop still peeks first and removes only after successful broker send.

## Operational notes

- Store instances are disposable; dispose them when they are not owned by the client/service
  provider lifetime.
- A locked, unreadable, truncated, or malformed database fails explicitly with
  `LiteDbStorageException`.
- The package is optimized for embedded, local durability. Use a custom store when durability must
  be shared across several processes or machines.
- Keep database files on reliable local storage. Avoid network shares for high-churn offline
  queues.

## Related docs

- [Resilience durable storage](/guide/resilience#durable-storage)
- [Extending stores](/guide/extending#message-store)
- [Package add-ons](/guide/package-add-ons)
