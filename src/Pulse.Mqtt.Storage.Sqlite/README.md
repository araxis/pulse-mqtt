# Pulse.Mqtt.Storage.Sqlite

Durable session and offline-message stores backed by SQLite. Use it when subscriptions, in-flight QoS state, and queued outbound publishes must survive process restarts in a local relational file store.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.Sqlite
```

## Configure with dependency injection

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSessionStore(_ => new SqliteSessionStore("telemetry-session.db"))
    .UseMessageStore(_ => new SqliteMessageStore(
        "telemetry-queue.db",
        new OfflineQueueOptions
        {
            Capacity = 10_000,
            Overflow = OverflowPolicy.Block,
            IncludeQos0 = false,
        }));
```

## Configure directly

```csharp
var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket
    {
        ClientId = "telemetry-worker",
        CleanStart = false,
        SessionExpiryInterval = 300,
    },
    SessionStore = new SqliteSessionStore("telemetry-session.db"),
    MessageStore = new SqliteMessageStore(
        "telemetry-queue.db",
        new OfflineQueueOptions { Capacity = 10_000 }),
};
```

The stores use the same `ISessionStore` and `IMessageStore` contracts as the in-memory defaults. Reconnect, publish, subscribe, and flush behavior do not change when the storage implementation changes.

Full docs: https://araxis.github.io/pulse-mqtt/packages/storage-sqlite
