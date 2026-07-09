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
        options.ClientId = "device-worker";
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default));
```

`Pulse.Mqtt.Core` comes in transitively. Reference it directly only when building on the raw
codec, raw client, or swap-point contracts without the resilient client.

## Pick add-ons by job

| Job | Add package | Package docs |
| --- | --- | --- |
| Host-managed clients, keyed DI, options binding, health checks | `Pulse.Mqtt.DependencyInjection` | [Dependency injection](/packages/dependency-injection) |
| Minimal-API-style topic endpoints | `Pulse.Mqtt.Endpoints` | [Endpoints](/packages/endpoints) |
| Durable relational session and queue storage | `Pulse.Mqtt.Storage.Sqlite` | [SQLite storage](/packages/storage-sqlite) |
| Durable server database session and queue storage | `Pulse.Mqtt.Storage.SqlServer` | [SQL Server storage](/packages/storage-sqlserver) |
| Durable embedded document session and queue storage | `Pulse.Mqtt.Storage.LiteDB` | [LiteDB storage](/packages/storage-litedb) |
| Bounded worker pipelines over messages, routes, acknowledgements, or state | `Pulse.Mqtt.Dataflow` | [Dataflow](/packages/dataflow) |
| Source-generated UTF-8 JSON typed payloads | `Pulse.Mqtt.Serialization.Json` | [JSON serializer](/packages/serialization-json) |
| Compact binary typed payloads with generated resolvers | `Pulse.Mqtt.Serialization.MessagePack` | [MessagePack serializer](/packages/serialization-messagepack) |
| Generated Protocol Buffers typed payloads | `Pulse.Mqtt.Serialization.Protobuf` | [Protobuf serializer](/packages/serialization-protobuf) |
| MQTT over `ws` or `wss` | `Pulse.Mqtt.Transport.WebSocket` | [WebSocket transport](/packages/transport-websocket) |
| MQTT over QUIC (EMQX-style listeners, .NET 10) | `Pulse.Mqtt.Transport.Quic` | [QUIC transport](/packages/transport-quic) |
| Reconnect timing owned by a resilience pipeline | `Pulse.Mqtt.Resilience.Polly` | [Reconnect policy](/packages/resilience-polly) |
| In-process workflow tests | `Pulse.Mqtt.Testing` | [Testing](/packages/testing) |
| Compile-time warnings for common mistakes | `Pulse.Mqtt.Analyzers` | [Analyzers](/packages/analyzers) |

The full package list, targets, and dependency relationships are in [Packages](/reference/packages).

## Common combinations

| Application shape | Packages |
| --- | --- |
| Hosted service with typed JSON | `Client`, `DependencyInjection`, `Serialization.Json` |
| Durable offline worker | `Client`, `DependencyInjection`, `Serialization.Json`, one storage package |
| Bounded processing pipeline | `Client`, `DependencyInjection`, `Dataflow`, one serializer |
| Browser/proxy broker endpoint | `Client`, `DependencyInjection`, `Transport.WebSocket`, one serializer |
| Integration-style tests | `Client`, `Testing`, optionally `DependencyInjection` |
| Strict application project | runtime packages plus `Analyzers` with `PrivateAssets="all"` |

## Choosing between similar add-ons

Choose one serializer per named client. Use JSON for readable payloads and broad interop,
MessagePack for compact generated binary contracts, and Protobuf when messages already come from
`.proto` definitions.

Choose one durable storage package per client in most applications. SQLite is a good default when
you want a relational file store and easy inspection. SQL Server is a good fit when durable client
state should live in managed database infrastructure. LiteDB is a good fit when an embedded document
database is already part of the application.

Use Dataflow when pipeline composition, bounded buffering, backpressure, completion, or fault
propagation matter. Use the client route and stream APIs directly when a simple handler or
`await foreach` loop is enough.

## Package docs

Every add-on has a dedicated page under [Package docs](/packages/). Those pages include install
commands, setup examples, behavior notes, limitations, and links to the deeper guide pages.
