# Testing

Code that talks MQTT is testable at full speed, with no Docker, no ports, and no sleeps. Two
tools make it so: the **in-process broker** and **injected time**.

## The in-process broker

`Pulse.Mqtt.Testing` ships `PulseMqttTestBroker` — a real MQTT 5 broker living in your test
process. It **is** a transport factory, so it plugs into the client like any other transport:

```csharp
await using var broker = new PulseMqttTestBroker();

await using var client = new ResilientMqttClient(broker, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "sut" },
});
await client.StartAsync(ct);
```

With dependency injection:

```csharp
services.AddPulseMqttClient("devices", o => { o.Host = "in-process"; o.ClientId = "sut"; })
    .UseTransportFactory(_ => broker);
```

The whole stack runs for real — handshake, keep-alive, QoS 1/2 acknowledgements,
subscriptions, topic matching, routing between clients — in memory, in milliseconds.

### Inject messages

```csharp
await broker.PublishAsync(new MqttPublishPacket
{
    Topic = "sensors/boiler-1/telemetry",
    Payload = JsonSerializer.SerializeToUtf8Bytes(reading),
});
// …assert your handler ran.
```

### Assert what the app published

```csharp
var published = await broker.ClientPublishes.ReadAsync(ct);
published.Topic.ShouldBe("alerts/overheat");
```

`ClientPublishes` is a channel of every PUBLISH any client sent, in arrival order.

### Multiple clients

Each client connecting through the broker gets its own session; messages route between them
through real topic matching (forwarded at up to QoS 1). Wire a publisher service and a
consumer service to the same broker and test their conversation.

### Scope

No retained messages, no session persistence, forwarding capped at QoS 1 — built for fast
deterministic tests, not broker conformance. Keep a handful of true end-to-end tests against a
containerized broker, the way this repository runs its own
[integration suite](https://github.com/araxis/pulse-mqtt/tree/main/tests/Pulse.Mqtt.IntegrationTests)
against Mosquitto via Testcontainers.

## Deterministic time

Every timeout, keep-alive, and backoff delay goes through `TimeProvider`. Pass
`FakeTimeProvider` and reconnect tests need no real waiting:

```csharp
var time = new FakeTimeProvider();
await using var client = new ResilientMqttClient(factory, options, time);

// …drop the connection, then:
time.Advance(TimeSpan.FromSeconds(30));   // the backoff elapses instantly
```

## Waiting on states, not sleeps

Assert against signals, never `Task.Delay`-and-hope:

```csharp
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
await foreach (var change in client.WatchState(timeout.Token))
{
    if (change.Current == ConnectionState.Connected) break;
}
```

::: tip Publish before connected?
The client is usable in every state — but a QoS 0 publish while still connecting is
**dropped by design** (`DroppedOffline`). Tests that publish immediately after starting should
first wait for `Connected`, or use QoS 1 (which queues).
:::

## Patterns from this repository

- **Handlers first**: route handlers and stores are plain code — test them directly, no broker
  at all.
- **Test broker for workflows**: pub/sub conversations, RPC pairs, route isolation.
- **Scripted transport for protocol edges**: the client test suite drives exact packet
  sequences (duplicate QoS 2 deliveries, unexpected acknowledgements) through a loopback
  transport pair.
- **Containers for the truth**: a small Mosquitto suite proves the stack against a real
  broker.
