# Pulse.Mqtt

A high-performance, resilient MQTT 5.0 / 3.1.1 client for modern .NET (`net8.0` + `net10.0`).

Most .NET MQTT clients leave the hard parts to the application: reconnecting, re-subscribing,
queueing while offline, routing topics to handlers, typed payloads. Pulse makes all of that
first-class — and every major behavior is **swappable** behind a small contract, so replacing
the reconnect policy with Polly, the offline store with a durable one, or TCP with WebSocket is
one line, not a fork.

**Verified Native AOT:** the full stack compiles with zero trim/AOT warnings and runs as a
3.15 MB self-contained native binary.

## Packages

| Package | What it is |
|---|---|
| `Pulse.Mqtt.Core` | Wire codec (all 15 control packets, v5 + v3.1.1), transports (TCP/TLS, in-memory loopback), the raw client, all swap-point contracts. Depends only on the BCL + `System.IO.Pipelines`. |
| `Pulse.Mqtt.Client` | The resilient client: supervisor, topic routing, typed messaging, request/response. |
| `Pulse.Mqtt.DependencyInjection` | `AddPulseMqttClient`, named clients, hosted lifecycle, health checks. |
| `Pulse.Mqtt.Serialization.Json` | Source-generated `System.Text.Json` payload serialization (AOT-safe). |
| `Pulse.Mqtt.Resilience.Polly` | Reconnect strategy backed by a Polly v8 `ResiliencePipeline`. |
| `Pulse.Mqtt.Transport.WebSocket` | MQTT over WebSocket (`ws`/`wss`). |
| `Pulse.Mqtt.Testing` | `PulseMqttTestBroker` — an in-process broker for millisecond tests with no Docker. |

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

var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("telemetry");
```

### Direct construction

```csharp
var factory = new TcpTransportFactory(new TcpTransportOptions { Host = "broker.example.com" });
await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "service-1" },
});
await client.StartAsync(ct); // connects in the background; watch client.State
```

### Route topics to handlers

```csharp
await client.OnAsync("sensors/{deviceId}/temp", (message, values, ct) =>
{
    Console.WriteLine($"{values["deviceId"]}: {Encoding.UTF8.GetString(message.Payload.Span)}");
    return ValueTask.CompletedTask;
});
// Subscribes sensors/+/temp, captures {deviceId}, dispatches through a bounded per-route queue.
```

### Typed messaging

```csharp
// options.Serializer = new JsonMqttSerializer(AppJsonContext.Default);
await client.PublishAsync("telemetry/1", new Reading("dev-1", 21.5));            // stamps content type
await client.OnAsync<Reading>("telemetry/{id}", (reading, msg, ct) => Handle(reading));
```

### Request / response

```csharp
var status = await client.RequestAsync<StatusQuery, StatusReply>("devices/7/status", new StatusQuery());
// Response topic + correlation data managed for you; concurrent calls never cross.

await client.OnRequestAsync<StatusQuery, StatusReply>("devices/{id}/status",
    (query, msg, ct) => ValueTask.FromResult(BuildStatus(msg.Values["id"])));
```

### Test without a broker

```csharp
await using var broker = new PulseMqttTestBroker();
await using var client = new ResilientMqttClient(broker, options);
// Full pub/sub, QoS acknowledgements, and routing between clients — in memory, in milliseconds.
```

## Swap any major behavior

| Behavior | Contract | Default | Swap example |
|---|---|---|---|
| Reconnect loop | `IReconnectStrategy` | Exponential backoff + jitter | `.UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline))` |
| Retry vs. fault | `IReconnectDecision` | Auth/identity reasons are final | Treat `NotAuthorized` as transient for token rotation |
| Connection up/down | `IConnectionLifecycle` | Re-subscribe from the session store | Add cache warming on reconnect |
| Session state | `ISessionStore` | In-memory | A durable store that survives restarts |
| Offline queue | `IMessageStore` | Bounded in-memory, 4 overflow policies | A durable queue |
| Payload format | `IMqttSerializer` | none (raw bytes) | JSON (source-gen), or your own |
| Transport | `IMqttTransportFactory` | TCP / TLS | WebSocket, or the in-memory test broker |

Terminal failures (for example a broker answering `NotAuthorized`) fault the client **sticky** —
it stops instead of retrying forever, and an explicit `StartAsync` recovers after the cause is
fixed.

## Performance

Measured with BenchmarkDotNet (`MemoryDiagnoser`) on .NET 10:

| Operation | Mean | Allocated |
|---|---|---|
| Publish encode (QoS 1, v5) | ~175 ns | 64 B |
| Frame + decode the same packet | ~93 ns | 312 B (the decoded objects themselves) |
| Topic filter match | ~32 ns | 0 B |
| Route template match (2 captures) | ~56 ns | 104 B (the captured values) |
| Variable-length integer round-trip | ~26 ns | 0 B |

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

MQTT over QUIC, MessagePack/Protobuf serializers, automatic topic-alias negotiation, fine-grained
flow-control tuning, and MQTT 5 enhanced authentication are tracked as backlog; the contracts
they plug into already exist.

## Documents

- [Development plan](docs/NG-MQTT-Client-Development-Plan.md)
- [Competitive research](docs/Competitive-Research-MQTT-Clients.md)
- [Resilience design](docs/Phase-04-Resilience-Detailed-Design.md)
- [Changelog](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
