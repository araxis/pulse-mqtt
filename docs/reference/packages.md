# Packages

Runtime packages target `net8.0` and `net10.0`, carry XML documentation, and are Native AOT
compatible. The analyzer package is a compiler extension and targets `netstandard2.0`. MIT licensed.
For package-specific setup, see [Package docs](/packages/). For a task-oriented install map, see
[Package add-ons](/guide/package-add-ons).

| Package | What it is | Depends on |
| --- | --- | --- |
| `Pulse.Mqtt.Core` | Wire codec (all 15 control packets, MQTT 5.0 + 3.1.1), transports (TCP/TLS, in-memory loopback), `MqttConnection`, `RawMqttClient`, every swap-point contract | BCL + `System.IO.Pipelines` only |
| `Pulse.Mqtt.Client` | `ResilientMqttClient`: supervision, topic routing, typed messaging, request/response, diagnostics | Core, `Microsoft.Extensions.Logging.Abstractions` |
| [`Pulse.Mqtt.Dataflow`](/packages/dataflow) | `ISourceBlock<T>` adapters for messages, routes, acknowledged routes, and state transitions | Client |
| [`Pulse.Mqtt.DependencyInjection`](/packages/dependency-injection) | `AddPulseMqttClient`, named clients, options binding, hosted lifecycle, health checks | Client + `Microsoft.Extensions.*` abstractions |
| [`Pulse.Mqtt.Endpoints`](/packages/endpoints) | Minimal-API-style `MapMqtt` endpoints with route constraints and per-message scopes | Client, DependencyInjection |
| [`Pulse.Mqtt.Serialization.Json`](/packages/serialization-json) | Source-generated `System.Text.Json` serializer | Client |
| [`Pulse.Mqtt.Serialization.MessagePack`](/packages/serialization-messagepack) | MessagePack serializer (compact binary, source-generated resolvers) | Core, `MessagePack` |
| [`Pulse.Mqtt.Serialization.Protobuf`](/packages/serialization-protobuf) | Protocol Buffers serializer (compact binary, explicit parser registry) | Core, Protobuf runtime |
| [`Pulse.Mqtt.Resilience.Polly`](/packages/resilience-polly) | `PollyReconnectStrategy` over a Polly v8 `ResiliencePipeline` | Core, `Polly.Core` |
| [`Pulse.Mqtt.Storage.LiteDB`](/packages/storage-litedb) | `LiteDbSessionStore` + `LiteDbMessageStore`: subscriptions, the offline queue, and in-flight QoS state survive restarts | Core, `LiteDB` |
| [`Pulse.Mqtt.Storage.SqlServer`](/packages/storage-sqlserver) | `SqlServerSessionStore` + `SqlServerMessageStore`: subscriptions, the offline queue, and in-flight QoS state survive restarts in SQL Server | Core, `Microsoft.Data.SqlClient` |
| [`Pulse.Mqtt.Storage.Sqlite`](/packages/storage-sqlite) | `SqliteSessionStore` + `SqliteMessageStore`: subscriptions, the offline queue, and in-flight QoS state survive restarts | Core, `Microsoft.Data.Sqlite` |
| [`Pulse.Mqtt.Transport.WebSocket`](/packages/transport-websocket) | MQTT over `ws`/`wss` | Core |
| [`Pulse.Mqtt.Transport.Quic`](/packages/transport-quic) | MQTT over QUIC, one bidirectional stream (.NET 10 only) | Core |
| [`Pulse.Mqtt.Testing`](/packages/testing) | `PulseMqttTestBroker`, the in-process broker with opt-in retained messages, persistent sessions, and scripted responses | Core |
| [`Pulse.Mqtt.Analyzers`](/packages/analyzers) | Optional C# diagnostics for common Pulse MQTT usage mistakes | none |

## Which do I need?

- **A typical service**: `Client` + `DependencyInjection` + `Serialization.Json`.
- **Minimal footprint / your own composition**: `Client` alone (or `Core` alone for the raw
  layers).
- **Tests**: add `Testing`.
- **Pipeline-style consumers**: add `Dataflow`.
- **Compact binary payloads**: add `Serialization.MessagePack` or `Serialization.Protobuf`.
- **Polly policies or WebSocket brokers**: add the matching add-on.
- **Compile-time guidance**: add `Analyzers` with `PrivateAssets="all"`.
