# Pulse.Mqtt.Storage.SqlServer

Durable session and offline-message stores backed by SQL Server. Use it when subscriptions, in-flight QoS state, and queued outbound publishes must survive process restarts in a server database.

## Install

```shell
dotnet add package Pulse.Mqtt.Storage.SqlServer
```

## Configure with dependency injection

```csharp
var storage = new SqlServerStorageOptions
{
    SchemaName = "mqtt",
    TablePrefix = "Device7",
};

builder.Services
    .AddPulseMqttClient("telemetry", configure)
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

## Configure directly

```csharp
var storage = new SqlServerStorageOptions { TablePrefix = "TelemetryWorker" };

var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket
    {
        ClientId = "telemetry-worker",
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

The connection string must point at an existing SQL Server database. The stores create their schema and tables on first use when the login has permission. Use a distinct `TablePrefix` per logical client or deployment slot so one client does not own another client's session and offline queue rows.

Full docs: https://araxis.github.io/pulse-mqtt/packages/storage-sqlserver
