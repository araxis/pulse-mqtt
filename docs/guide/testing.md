# Testing

Code that talks MQTT is testable at full speed, with no Docker, no ports, and no sleeps. Two
tools make it so: the **in-process broker** and **injected time**.

## The in-process broker

`Pulse.Mqtt.Testing` ships `PulseMqttTestBroker` — an in-process MQTT 5.0 / 3.1.1 broker
living in your test process. It **is** a transport factory, so it plugs into the client like
any other transport:

```csharp
await using var broker = new PulseMqttTestBroker();

await using var client = new ResilientMqttClient(broker, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "sut" },
});
await client.ConnectAsync(ct);
```

With dependency injection:

```csharp
services.AddPulseMqttClient("devices", o => { o.Host = "in-process"; o.ClientId = "sut"; })
    .UseTransportFactory(_ => broker);
```

The whole stack runs for real — handshake, keep-alive, QoS acknowledgements, subscriptions,
topic matching, routing between clients — in memory, in milliseconds.

The default constructor keeps the broker lightweight and backward compatible: retained storage
and persistent sessions are off, and forwarded messages are capped at QoS 1. Turn on more
realistic behavior only in tests that need it:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    RetainedMessages = true,
    PersistentSessions = true,
    MaximumForwardQualityOfService = MqttQualityOfService.ExactlyOnce,
});
```

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
through real topic matching. Wire a publisher service and a consumer service to the same broker
and test their conversation. By default forwarded messages are capped at QoS 1; set
`MaximumForwardQualityOfService = MqttQualityOfService.ExactlyOnce` when the test needs the
broker-to-client QoS 2 exchange.

### Retained messages

Retained storage is opt-in:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    RetainedMessages = true,
});
```

When enabled, a retained publish is stored by exact topic and replayed to later matching
subscriptions. A retained publish with an empty payload clears the stored value. MQTT 5
subscription flags are honored: `RetainHandling` controls replay, and `RetainAsPublished`
preserves the retain flag on live forwarded messages.

### Persistent sessions

Persistent sessions are opt-in:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    PersistentSessions = true,
});
```

When enabled, subscriptions survive reconnects for clients that use a non-empty client id and
`CleanStart = false`. A clean-start connection or MQTT 5 `SessionExpiryInterval = 0` clears the
stored session. The older `ResumeSessions = true` shortcut now maps to this same behavior.

### Scripted broker responses

Use scripted responses when a workflow test needs a broker policy or fault without running a
separate broker:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    ConnAckFactory = context => context.DefaultConnAck with
    {
        ReasonCode = MqttReasonCode.NotAuthorized,
        ReasonString = "test rejection",
    },
});
```

`ConnAckFactory` can accept or reject CONNECT, and successful custom values such as
`ReceiveMaximum`, `ServerReference`, or `AssignedClientIdentifier` are visible to the client.
A rejected connection is closed and does not create persistent session state.

`SubAckFactory` can grant some filters and deny others:

```csharp
SubAckFactory = context => context.DefaultSubAck with
{
    ReasonCodes =
    [
        MqttReasonCode.NotAuthorized,
        MqttReasonCode.GrantedQualityOfService1,
    ],
};
```

The returned SUBACK must include one reason code per requested filter. Only granted filters are
stored, so denied filters do not receive retained replay and are not resumed in persistent
sessions.

`PublishAckFactory` can fail or withhold the first QoS 1/2 acknowledgement:

```csharp
PublishAckFactory = context => context.Publish.Topic == "slow/path"
    ? null
    : context.DefaultAcknowledgement;
```

Returning `null` leaves the publish unacknowledged so the client timeout path can be tested.
Returning a failure reason, such as `NotAuthorized`, surfaces to the publisher and the broker
does not route or retain that publish. QoS 0 publishes still have no acknowledgement.

To simulate a broker-initiated connection loss, close one client id or every connected session:

```csharp
await broker.DisconnectClientAsync("device-7", new MqttDisconnectPacket
{
    ReasonCode = MqttReasonCode.ServerMoved,
    ServerReference = "mqtt://next-broker",
}, ct);

await broker.DisconnectAllAsync(cancellationToken: ct);
```

### Scope

The broker is built for fast deterministic workflow tests, not broker conformance. It covers
MQTT 5.0 and 3.1.1 handshakes, subscriptions, retained replay, persistent sessions, QoS
acknowledgements, scripted response hooks, broker-initiated disconnects, and in-memory routing,
but it does not try to model every broker policy or failure. Keep a handful of true end-to-end
tests against a containerized broker, the way this
repository runs its own
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
await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(10), timeout.Token);
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
- **Scripted test broker for policy edges**: rejected connects, denied subscriptions,
  publish-ack failures, and broker disconnects.
- **Scripted transport for packet edges**: the client test suite still drives exact packet
  sequences such as duplicate QoS 2 deliveries through a loopback transport pair.
- **Containers for the truth**: a small Mosquitto suite proves the stack against a real
  broker.
