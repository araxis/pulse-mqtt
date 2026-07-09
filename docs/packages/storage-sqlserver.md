# SQL Server storage package

Package: `Pulse.Mqtt.Storage.SqlServer`

Use this package when a client needs durable session state and an offline queue backed by SQL
Server rather than a local embedded store.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.SqlServer
```

## What it provides

| Type | Contract | Stores |
| --- | --- | --- |
| `SqlServerSessionStore` | `ISessionStore` | Durable subscriptions and in-flight QoS state. |
| `SqlServerMessageStore` | `IMessageStore` | Offline outbound publishes in FIFO order. |
| `SqlServerStorageOptions` | Options | Database schema and table prefix naming. |
| `SqlServerStorageException` | Exception | Database open, schema, permission, and malformed stored-packet failures. |

The stores use the same contracts as the in-memory defaults. Reconnect, publish, subscribe, and
flush behavior do not change when the storage implementation changes.

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Storage.SqlServer;

var storage = new SqlServerStorageOptions
{
    SchemaName = "mqtt",
    TablePrefix = "Device7",
};

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseSessionStore(_ => new SqlServerSessionStore(connectionString, storage))
    .UseMessageStore(_ => new SqlServerMessageStore(
        connectionString,
        new OfflineQueueOptions
        {
            Capacity = 10_000,
            Overflow = OverflowPolicy.Block,
            IncludeQos0 = false,
        },
        storage));
```

The connection string must target an existing SQL Server database. The package creates its schema
and tables on first use when the login has permission.

## Configure directly

```csharp
var storage = new SqlServerStorageOptions
{
    SchemaName = "mqtt",
    TablePrefix = "Device7",
};

var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket
    {
        ClientId = "device-worker",
        CleanStart = false,
        SessionExpiryInterval = 300,
    },
    SessionStore = new SqlServerSessionStore(connectionString, storage),
    MessageStore = new SqlServerMessageStore(
        connectionString,
        new OfflineQueueOptions { Capacity = 10_000 },
        storage),
};
```

Set `CleanStart = false` and a non-zero session expiry when the broker should preserve session
state across reconnects. The local session store preserves the client's view of subscriptions and
in-flight QoS state across process restarts.

## Table ownership

By default, the package uses schema `dbo` and table names starting with `PulseMqtt`:

- `PulseMqttSubscriptions`
- `PulseMqttInFlightOutbound`
- `PulseMqttInFlightInbound`
- `PulseMqttQueue`

Use `SqlServerStorageOptions.TablePrefix` to isolate logical clients, deployment slots, tenants, or
test runs that share one database. One logical client should own one prefix. Sharing a prefix across
two active clients means they are sharing the same session snapshot and offline queue.

## Offline queue behavior

`SqlServerMessageStore` follows the same `IMessageStore` contract as the in-memory queue:

- `EnqueueAsync` writes a queued publish.
- `PeekAsync` reads the oldest publish without removing it.
- `RemoveHeadAsync` removes the publish after a successful flush.
- `DroppedCount` reports overflow drops.

The peek-then-remove flow favors at-least-once recovery: if the process stops after send and before
remove, the publish can be retried after restart.

## Operational notes

- Store instances are disposable; dispose them when they are not owned by the client/service
  provider lifetime.
- The package opens short-lived SQL connections per operation and serializes store operations per
  store instance.
- Identifiers are quoted, and blank or over-length schema/prefix values are rejected before any SQL
  runs.
- A missing database, denied schema/table permission, unavailable server, or malformed stored packet
  fails explicitly with `SqlServerStorageException`.
- SQL Server is a good fit when durability should live in managed database infrastructure. SQLite or
  LiteDB remain simpler for single-process local storage.

## Related docs

- [Resilience durable storage](/guide/resilience#durable-storage)
- [Extending stores](/guide/extending#message-store)
- [Package add-ons](/guide/package-add-ons)
