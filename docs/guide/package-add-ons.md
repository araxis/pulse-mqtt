# Package add-ons

Pulse.Mqtt keeps the resilient client small and adds focused packages for hosting, storage,
payload formats, pipelines, transport, tests, and compile-time guidance. Add only the packages a
project actually uses.

## Start with the client

Most services start with the client, hosting integration, and one serializer:

```shell
dotnet add package Pulse.Mqtt.Client
dotnet add package Pulse.Mqtt.DependencyInjection
dotnet add package Pulse.Mqtt.Serialization.Json
```

```csharp
builder.Services
    .AddPulseMqttClient("devices", options =>
    {
        options.Host = "broker.example.com";
        options.Port = 1883;
        options.ClientId = "device-worker";
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default));
```

`Pulse.Mqtt.Core` comes in transitively. Reference it directly only when building on the raw
codec, raw client, or swap-point contracts without the resilient client.

## Pick add-ons by job

| Job | Add package | Use when |
| --- | --- | --- |
| Host-managed clients | `Pulse.Mqtt.DependencyInjection` | You want named clients, options binding, hosted lifecycle, and health checks. |
| Durable sessions and offline queue | `Pulse.Mqtt.Storage.Sqlite` or `Pulse.Mqtt.Storage.LiteDB` | Subscriptions, in-flight QoS state, and queued publishes must survive process restart. |
| Bounded processing pipelines | `Pulse.Mqtt.Dataflow` | Messages, routes, acknowledged routes, or state changes feed source blocks. |
| Typed JSON payloads | `Pulse.Mqtt.Serialization.Json` | You want source-generated JSON for typed publish, route, stream, and request/response APIs. |
| Compact binary payloads | `Pulse.Mqtt.Serialization.MessagePack` or `Pulse.Mqtt.Serialization.Protobuf` | You want smaller generated binary payloads. |
| MQTT over WebSocket | `Pulse.Mqtt.Transport.WebSocket` | The broker is exposed through `ws` or `wss`, often behind a proxy or gateway. |
| Custom reconnect policy | `Pulse.Mqtt.Resilience.Polly` | Reconnect attempts should be driven by an existing resilience pipeline. |
| In-process workflow tests | `Pulse.Mqtt.Testing` | Tests need a broker in the same process, with optional retained messages, persistent sessions, and scripted responses. |
| Compiler warnings | `Pulse.Mqtt.Analyzers` | You want warnings for unawaited operations, missing cancellation tokens, and sync disposal mistakes. |

Dedicated package pages are in [Package docs](/packages/). The full package list, targets, and
dependencies are in [Packages](/reference/packages).

## Durable storage

The default stores are in-memory. Add one durable store package when a client must resume its
subscription set, in-flight QoS state, and offline queue after a process restart:

```shell
dotnet add package Pulse.Mqtt.Storage.Sqlite
# or
dotnet add package Pulse.Mqtt.Storage.LiteDB
```

With dependency injection:

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

The document-store package has the same shape:

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

Both packages use the same `ISessionStore` and `IMessageStore` contracts as the in-memory
defaults, so publishing, subscribing, and reconnect behavior does not change. See
[Resilience](./resilience#durable-storage) for the restart and redelivery details.

## Pipeline source blocks

Add the Dataflow package when MQTT input should feed bounded pipeline blocks:

```shell
dotnet add package Pulse.Mqtt.Dataflow
```

```csharp
using Pulse.Mqtt.Dataflow;
using System.Threading.Tasks.Dataflow;

var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

await using var source = client.ToRouteSourceBlock(
    template,
    sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 128 },
    cancellationToken: token);

using var link = source.LinkTo(
    new ActionBlock<MqttRoutedMessage>(
        routed => ProcessAsync(routed, token),
        new ExecutionDataflowBlockOptions { BoundedCapacity = 64 }),
    new DataflowLinkOptions { PropagateCompletion = true });
```

Route source blocks are local adapters. They do not subscribe to the broker; call
`SubscribeAsync` first. Raw message source blocks consume `client.Messages`, so do not read the
raw stream directly from another place at the same time. See [Routing](./routing#dataflow-source-blocks)
and [Subscribing](./subscribing#consuming-the-raw-message-stream).

## Typed payload serializers

One serializer is configured per client. The typed publish, route, stream, and request/response
APIs all use it.

```shell
dotnet add package Pulse.Mqtt.Serialization.Json
dotnet add package Pulse.Mqtt.Serialization.MessagePack
dotnet add package Pulse.Mqtt.Serialization.Protobuf
```

JSON:

```csharp
.UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default))
```

MessagePack:

```csharp
var serializer = new MessagePackMqttSerializer(messagePackOptions);
```

Protocol Buffers:

```csharp
var registry = ProtobufMessageRegistry.Create(registry =>
{
    registry.Add(TelemetryReading.Parser);
});

var serializer = new ProtobufMqttSerializer(registry);
```

Use generated serializers or explicit parser registration for trimming and Native AOT. See
[Typed messaging](./typed-messaging) for full setup, metadata, and limitations.

## Transport and reconnect add-ons

Use the WebSocket transport when TCP is not the broker-facing path:

```shell
dotnet add package Pulse.Mqtt.Transport.WebSocket
```

```csharp
.UseTransportFactory(_ => new WebSocketTransportFactory(new WebSocketTransportOptions
{
    Uri = new Uri("wss://broker.example.com/mqtt"),
}))
```

Use the reconnect add-on when retry timing should be owned by an existing resilience pipeline:

```shell
dotnet add package Pulse.Mqtt.Resilience.Polly
```

```csharp
.UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline))
```

See [Connecting](./connecting#websocket) and [Resilience](./resilience#backoff) for the behavior
around proxy headers, terminal failures, and retry classification.

## Tests and analyzers

For workflow tests:

```shell
dotnet add package Pulse.Mqtt.Testing
```

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    RetainedMessages = true,
    PersistentSessions = true,
});
```

The broker is an `IMqttTransportFactory`, so production client code can use it in tests without a
network port. It can also script rejected connects, denied subscriptions, publish
acknowledgement failures, timeouts, and broker-initiated disconnects. See [Testing](./testing).

For compiler guidance:

```shell
dotnet add package Pulse.Mqtt.Analyzers
```

Keep analyzer references private in project files. If the project does not use central package
management, keep the version that `dotnet add package` inserted:

```xml
<PackageReference Include="Pulse.Mqtt.Analyzers" PrivateAssets="all" />
```

See [Analyzers](./analyzers) for diagnostic IDs, fixes, and suppression options.
