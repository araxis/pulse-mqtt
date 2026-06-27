# Pulse.Mqtt

[![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Client?logo=nuget&label=Pulse.Mqtt.Client)](https://www.nuget.org/packages/Pulse.Mqtt.Client)
[![Downloads](https://img.shields.io/nuget/dt/Pulse.Mqtt.Client?logo=nuget&label=downloads)](https://www.nuget.org/packages/Pulse.Mqtt.Client)
[![CI](https://github.com/araxis/pulse-mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/araxis/pulse-mqtt/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%2010.0-512BD4?logo=dotnet)](https://dotnet.microsoft.com)
[![Docs](https://img.shields.io/badge/docs-pulse--mqtt-1f6feb)](https://araxis.github.io/pulse-mqtt/)

A high-performance, resilient MQTT 5.0 / 3.1.1 client for modern .NET (`net8.0` + `net10.0`).

Most .NET MQTT clients leave the hard parts to the application: reconnecting, re-subscribing,
queueing while offline, routing topics to handlers, typed payloads. Pulse makes all of that
first-class — and every major behavior is **swappable** behind a small contract, so replacing
the reconnect policy with Polly, the offline store with a durable one, or TCP with WebSocket is
one line, not a fork.

**Verified Native AOT:** the full stack compiles with zero trim/AOT warnings and runs as a
3.15 MB self-contained native binary.

## Packages

| Package | NuGet | What it is |
|---|---|---|
| `Pulse.Mqtt.Core` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Core?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Core) | Wire codec (all 15 control packets, v5 + v3.1.1), transports (TCP/TLS, in-memory loopback), the raw client, all swap-point contracts. Depends only on the BCL + `System.IO.Pipelines`. |
| `Pulse.Mqtt.Client` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Client?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Client) | The resilient client: supervisor, topic routing, typed messaging, request/response. |
| `Pulse.Mqtt.Dataflow` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Dataflow?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Dataflow) | Dataflow source blocks for client messages, routes, acknowledged routes, and state transitions. |
| `Pulse.Mqtt.DependencyInjection` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.DependencyInjection?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.DependencyInjection) | `AddPulseMqttClient`, named clients, hosted lifecycle, health checks. |
| `Pulse.Mqtt.Serialization.Json` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Serialization.Json?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Serialization.Json) | Source-generated `System.Text.Json` payload serialization (AOT-safe). |
| `Pulse.Mqtt.Serialization.MessagePack` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Serialization.MessagePack?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Serialization.MessagePack) | MessagePack payload serialization for compact binary messages. |
| `Pulse.Mqtt.Serialization.Protobuf` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Serialization.Protobuf?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Serialization.Protobuf) | Protocol Buffers payload serialization for generated binary messages. |
| `Pulse.Mqtt.Resilience.Polly` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Resilience.Polly?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Resilience.Polly) | Reconnect strategy backed by a Polly v8 `ResiliencePipeline`. |
| `Pulse.Mqtt.Storage.LiteDB` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Storage.LiteDB?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Storage.LiteDB) | Durable LiteDB session and offline-message stores. |
| `Pulse.Mqtt.Storage.Sqlite` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Storage.Sqlite?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Storage.Sqlite) | Durable SQLite session and offline-message stores. |
| `Pulse.Mqtt.Transport.WebSocket` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Transport.WebSocket?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Transport.WebSocket) | MQTT over WebSocket (`ws`/`wss`). |
| `Pulse.Mqtt.Testing` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Testing?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Testing) | `PulseMqttTestBroker` — an in-process broker for millisecond tests, with opt-in retained messages, persistent sessions, and scripted responses. |
| `Pulse.Mqtt.Analyzers` | [![NuGet](https://img.shields.io/nuget/v/Pulse.Mqtt.Analyzers?logo=nuget&label=%20)](https://www.nuget.org/packages/Pulse.Mqtt.Analyzers) | Optional C# diagnostics for common Pulse MQTT usage mistakes. |

## Quick start

### With dependency injection

```csharp
services.AddPulseMqttClient("telemetry", options =>
{
    options.Host = "broker.example.com";
    options.Port = 8883;
    options.UseTls = true;
    options.ClientId = "service-1";
});
// The client connects with the host, reconnects on drops, re-subscribes, and reports health.
// Prefer manual control? options.ConnectWithHost = false, then ConnectAsync/DisconnectAsync at will.

var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("telemetry");
```

### Direct construction

```csharp
var factory = new TcpTransportFactory(new TcpTransportOptions { Host = "broker.example.com" });
await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "service-1" },
});
await client.ConnectAsync(ct); // connects in the background
await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(10), ct); // when readiness matters
```

### Route topics to handlers

```csharp
var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);

using var route = client.RegisterRoute(template, (message, values, ct) =>
{
    Console.WriteLine($"{values["deviceId"]}: {Encoding.UTF8.GetString(message.Payload.Span)}");
    return ValueTask.CompletedTask;
});
// SubscribeAsync owns broker delivery; RegisterRoute owns local dispatch and captured values.
```

Need broker acknowledgement to wait for application work? Use
`OpenAcknowledgedRouteStream(...)` and call `AcknowledgeAsync` or `RejectAsync` after handling
the routed message. `RejectAsync` is available when `CanReject` is true, which means the
delivery can carry an MQTT 5 negative acknowledgement reason code.

### Typed messaging

```csharp
// options.Serializer = new JsonMqttSerializer(AppJsonContext.Default);
await client.PublishAsync("telemetry/1", new Reading("dev-1", 21.5));            // stamps content type
var template = MqttRouteTemplate.Parse("telemetry/{id}");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);
using var route = client.RegisterRoute<Reading>(template, (reading, msg, ct) => Handle(reading));
```

### Request / response

```csharp
var status = await client.RequestAsync<StatusQuery, StatusReply>("devices/7/status", new StatusQuery());
// Response topic + correlation data managed for you; concurrent calls never cross.

var template = MqttRouteTemplate.Parse("devices/{id}/status");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);
using var responder = client.RegisterRequestHandler<StatusQuery, StatusReply>(template,
    (query, msg, ct) => ValueTask.FromResult(BuildStatus(msg.Values["id"])));
```

### Or do it all fluently

```csharp
await using var client = await new PulseMqttClientBuilder()
    .WithTcp("broker.example.com", 8883, useTls: true)
    .WithClientId("service-1")
    .WithSerializer(new JsonMqttSerializer(AppJsonContext.Default))
    .BuildAndConnectAsync(ct);

await client.Publish("telemetry/1").AtLeastOnce().WithRetain()
    .WithPayload(new Reading("dev-1", 21.5)).SendAsync(ct);

var route = client.Route("sensors/{deviceId}/temp");
await client.SubscribeAsync([route.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);
using var registration = route
    .WithConcurrency(4).Handle<Reading>((reading, msg, ct) => Handle(reading));
```

### Test without a broker

```csharp
await using var broker = new PulseMqttTestBroker();
await using var client = new ResilientMqttClient(broker, options);
// Full pub/sub, QoS acknowledgements, and routing between clients — in memory, in milliseconds.
```

Need retained-message, persistent-session, denied subscription, rejected connection, publish
acknowledgement failure, or broker-disconnect behavior in a workflow test? Pass
`PulseMqttTestBrokerOptions` and keep the same client setup.

## Swap any major behavior

| Behavior | Contract | Default | Swap example |
|---|---|---|---|
| Reconnect loop | `IReconnectStrategy` | Exponential backoff + jitter | `.UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline))` |
| Retry vs. fault | `IReconnectDecision` | Auth/identity reasons are final | Treat `NotAuthorized` as transient for token rotation |
| Connection up/down | `IConnectionLifecycle` | Re-subscribe from the session store | Add cache warming on reconnect |
| Session state | `ISessionStore` | In-memory | A durable store that survives restarts |
| Offline queue | `IMessageStore` | Bounded in-memory, 4 overflow policies | A durable queue |
| Payload format | `IMqttSerializer` | none (raw bytes) | JSON, MessagePack, Protobuf, or your own |
| Transport | `IMqttTransportFactory` | TCP / TLS | WebSocket, or the in-memory test broker |

Terminal failures (for example a broker answering `NotAuthorized`) fault the client **sticky** —
it stops instead of retrying forever, and an explicit `ConnectAsync` recovers after the cause is
fixed.

## Sample

[`samples/Pulse.Mqtt.Sample`](samples/Pulse.Mqtt.Sample) is a runnable console app covering
hosting, typed publishes, routed subscriptions, and request/response. It needs no
infrastructure — without arguments it runs against the in-process test broker:

```
dotnet run --project samples/Pulse.Mqtt.Sample
dotnet run --project samples/Pulse.Mqtt.Sample -- --host localhost --port 1883
```

[`samples/Pulse.Mqtt.AspNetCoreSample`](samples/Pulse.Mqtt.AspNetCoreSample) is a Minimal API
host covering named/keyed dependency injection, health checks, diagnostics snapshots, typed
publishing, routed consumption, and safe broker-capability checks for MQTT 5-only behavior:

```
dotnet run --project samples/Pulse.Mqtt.AspNetCoreSample
dotnet run --project samples/Pulse.Mqtt.AspNetCoreSample -- --Mqtt:Host localhost --Mqtt:Port 1883
```

[`samples/Pulse.Mqtt.WorkerSample`](samples/Pulse.Mqtt.WorkerSample) is a worker pipeline using
bounded Dataflow source blocks, explicit subscriptions, graceful shutdown, and capability checks:

```
dotnet run --project samples/Pulse.Mqtt.WorkerSample
dotnet run --project samples/Pulse.Mqtt.WorkerSample -- --Mqtt:Host localhost --Mqtt:Port 1883
```

## Performance

Measured with BenchmarkDotNet (`MemoryDiagnoser`) on .NET 10:

| Operation | Mean | Allocated |
|---|---|---|
| Publish encode (v5, no properties) | ~60 ns | 0 B |
| Frame + decode the same packet | ~96 ns | 144 B (the decoded objects themselves) |
| Topic filter match | ~32 ns | 0 B |
| Route template match (2 captures) | ~56 ns | 104 B (the captured values) |
| Variable-length integer round-trip | ~26 ns | 0 B |

Head-to-head against MQTTnet over a real broker — lower allocations in every measured
scenario, comparable throughput, and protocol-compliance differences:
[the full comparison](docs/Benchmark-vs-MQTTnet.md).

Everything is bounded: inbound queues, per-route queues, the offline queue. Backpressure flows
to the socket instead of buffering without limits. All timing goes through `TimeProvider`, so
the whole stack is testable with a fake clock.

## Verification

- 300+ unit tests (codec round-trips and fuzzing, QoS state machines, reconnect scenarios,
  routing isolation, RPC) plus integration tests against a real Mosquitto broker.
- The decoder never throws anything but `MqttProtocolException`, fuzz-proven on tens of
  thousands of malformed inputs.
- Native AOT: zero warnings; the published native smoke binary runs the full stack.

## Scope notes

MQTT over QUIC, broker-side feature probes, and more protocol-specific tooling are tracked as
post-1.0 horizon items.

## Documentation

The full documentation is a VitePress site under [`docs/`](docs) — run it locally with
`cd docs && npm install && npm run docs:dev`, or read the pages directly:

**Guides**
- [Introduction](docs/guide/introduction.md) · [Getting started](docs/guide/getting-started.md) · [Package add-ons](docs/guide/package-add-ons.md) · [Connecting](docs/guide/connecting.md)
- [Publishing](docs/guide/publishing.md) · [Subscribing](docs/guide/subscribing.md) · [Routing](docs/guide/routing.md) · [Typed messaging](docs/guide/typed-messaging.md) · [Request and response](docs/guide/request-response.md)
- [Resilience](docs/guide/resilience.md) · [Lifecycle and state](docs/guide/lifecycle.md) · [Dependency injection](docs/guide/dependency-injection.md) · [Observability](docs/guide/observability.md) · [Testing](docs/guide/testing.md) · [Analyzers](docs/guide/analyzers.md)
- [Extending the client](docs/guide/extending.md) · [The raw client](docs/guide/raw-client.md) · [Native AOT](docs/guide/native-aot.md) · [Performance](docs/guide/performance.md) · [Releasing](docs/guide/releasing.md)

**Reference**
- [Package docs](docs/packages/index.md) · [Packages](docs/reference/packages.md) · [Options](docs/reference/options.md) · [MQTT protocol compatibility](docs/reference/protocol-compatibility.md) · [Connection states](docs/reference/connection-states.md) · [Errors](docs/reference/errors.md)

**Project**
- [Benchmark suite](docs/Benchmark-Suite.md) · [MQTTnet comparison](docs/Benchmark-vs-MQTTnet.md)
- [Development plan](docs/NG-MQTT-Client-Development-Plan.md) · [competitive research](docs/Competitive-Research-MQTT-Clients.md) · [resilience design](docs/Phase-04-Resilience-Detailed-Design.md)
- [Changelog](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
