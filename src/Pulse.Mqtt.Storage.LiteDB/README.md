# Pulse.Mqtt.Storage.LiteDB

Durable session and offline-message stores backed by LiteDB. Use it when subscriptions, in-flight QoS state, and queued outbound publishes must survive process restarts.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.LiteDB
```

## Configure with dependency injection

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSessionStore(_ => new LiteDbSessionStore("telemetry-session.db"))
    .UseMessageStore(_ => new LiteDbMessageStore(
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
    SessionStore = new LiteDbSessionStore("telemetry-session.db"),
    MessageStore = new LiteDbMessageStore(
        "telemetry-queue.db",
        new OfflineQueueOptions { Capacity = 10_000 }),
};
```

Plain file paths and LiteDB connection strings are accepted. Store instances are disposable when not owned by the service provider or client lifetime.

Full docs: https://araxis.github.io/pulse-mqtt/packages/storage-litedb
