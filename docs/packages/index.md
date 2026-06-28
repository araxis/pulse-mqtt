# Package docs

Pulse.Mqtt ships as focused packages. Start with the client packages, then add storage,
pipeline, serializer, transport, test, or analyzer packages only when the project needs them.

| Package | Docs | Use when |
| --- | --- | --- |
| `Pulse.Mqtt.Core` | [Reference](/reference/packages) | You need the codec, raw client, transports, or swap-point contracts without the resilient client. |
| `Pulse.Mqtt.Client` | [Getting started](/guide/getting-started) | You need the resilient client, routing, typed messaging, request/response, and diagnostics. |
| `Pulse.Mqtt.DependencyInjection` | [Dependency injection](./dependency-injection) | You want named clients, host lifecycle, options binding, and health checks. |
| `Pulse.Mqtt.Dataflow` | [Dataflow](./dataflow) | MQTT input should feed bounded source blocks and pipeline consumers. |
| `Pulse.Mqtt.Storage.Sqlite` | [SQLite storage](./storage-sqlite) | Session state and offline queue need a durable relational file store. |
| `Pulse.Mqtt.Storage.LiteDB` | [LiteDB storage](./storage-litedb) | Session state and offline queue need a durable embedded document store. |
| `Pulse.Mqtt.Serialization.Json` | [JSON serializer](./serialization-json) | Typed payloads use source-generated JSON. |
| `Pulse.Mqtt.Serialization.MessagePack` | [MessagePack serializer](./serialization-messagepack) | Typed payloads use compact binary messages. |
| `Pulse.Mqtt.Serialization.Protobuf` | [Protobuf serializer](./serialization-protobuf) | Typed payloads use generated Protocol Buffers messages. |
| `Pulse.Mqtt.Transport.WebSocket` | [WebSocket transport](./transport-websocket) | The broker is reached through `ws` or `wss`. |
| `Pulse.Mqtt.Resilience.Polly` | [Reconnect policy](./resilience-polly) | Reconnect timing should be owned by a resilience pipeline. |
| `Pulse.Mqtt.Testing` | [Testing](./testing) | Tests need an in-process broker. |
| `Pulse.Mqtt.Analyzers` | [Analyzers](./analyzers) | Projects should get compile-time warnings for common MQTT usage mistakes. |

For a task-oriented install map, see [Package add-ons](/guide/package-add-ons). For targets and
dependencies, see [Packages](/reference/packages).

Serializer packages also have a [serializer overview](./serializers) when you need to choose
between JSON, MessagePack, and Protobuf.
