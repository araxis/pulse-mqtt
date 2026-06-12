# Testing applications that use Pulse

`Pulse.Mqtt.Testing` ships `PulseMqttTestBroker`: an in-process MQTT 5 broker that plugs in as
a transport factory. Tests get the full client stack — handshake, keep-alive, QoS
acknowledgements, subscriptions, routing between clients — with no external process, no ports,
and no flakiness.

```csharp
await using var broker = new PulseMqttTestBroker();

await using var client = new ResilientMqttClient(broker, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "sut" },
});
await client.StartAsync(ct);
```

With dependency injection, swap it in through the builder:

```csharp
services.AddPulseMqttClient("devices", o => { o.Host = "in-process"; o.ClientId = "sut"; })
    .UseTransportFactory(_ => broker);
```

## Driving the test

- **Inject a message to subscribers**: `await broker.PublishAsync(new MqttPublishPacket { Topic = "...", Payload = ... })`.
- **Assert what the app published**: read `broker.ClientPublishes`, a channel of every PUBLISH
  any client sent, in arrival order.
- **Multiple clients**: each `ConnectAsync` gets its own session; messages route between them
  through real topic matching at QoS up to 1.

Scope: MQTT 5 sessions, no retained messages, no persistence — built for fast deterministic
tests, not broker conformance. Pair it with a containerized broker for the handful of true
end-to-end tests, the way this repository's own integration suite does.

## Determinism

The whole engine takes a `TimeProvider`. Pass `FakeTimeProvider` in tests and keep-alive,
reconnect backoff, and request timeouts become instant and deterministic.
