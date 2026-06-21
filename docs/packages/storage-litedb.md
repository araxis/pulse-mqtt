# LiteDB storage package

Package: `Pulse.Mqtt.Storage.LiteDB`

Use this package when a client needs durable session state and an offline queue backed by an
embedded document store.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.LiteDB
```

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Storage.LiteDB;

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseSessionStore(_ => new LiteDbSessionStore("devices-session.db"))
    .UseMessageStore(_ => new LiteDbMessageStore(
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
    SessionStore = new LiteDbSessionStore("devices-session.db"),
    MessageStore = new LiteDbMessageStore(
        "devices-queue.db",
        new OfflineQueueOptions { Capacity = 1024 }),
};
```

## Behavior

- `LiteDbSessionStore` persists subscriptions and in-flight QoS state.
- `LiteDbMessageStore` persists queued outbound publishes and preserves FIFO order.
- Both stores accept a plain file path or connection string.
- Missing, locked, or unreadable databases fail fast with `LiteDbStorageException`.

See [Resilience](/guide/resilience#durable-storage) for reconnect and redelivery behavior.
