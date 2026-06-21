# SQLite storage package

Package: `Pulse.Mqtt.Storage.Sqlite`

Use this package when a client needs durable session state and an offline queue backed by a
relational file store.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.Sqlite
```

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Storage.Sqlite;

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseSessionStore(_ => new SqliteSessionStore("devices-session.db"))
    .UseMessageStore(_ => new SqliteMessageStore(
        "devices-queue.db",
        new OfflineQueueOptions { Capacity = 1024 }));
```

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
        new OfflineQueueOptions { Capacity = 1024 }),
};
```

## Behavior

- `SqliteSessionStore` persists subscriptions and in-flight QoS state.
- `SqliteMessageStore` persists queued outbound publishes and preserves FIFO order.
- Both stores accept a plain file path or connection string.
- Missing, locked, or unreadable databases fail fast with `SqliteStorageException`.

See [Resilience](/guide/resilience#durable-storage) for reconnect and redelivery behavior.
