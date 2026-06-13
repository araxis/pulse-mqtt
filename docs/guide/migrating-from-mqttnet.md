# Migrating from MQTTnet

This maps the MQTTnet patterns you already know to their Pulse equivalents. The headline
difference: MQTTnet gives you a low-level `IMqttClient` and a separate `IManagedMqttClient` for
reconnect/queueing; Pulse's [`ResilientMqttClient`](./resilience) **is** the managed client —
reconnect, re-subscription, and the offline queue are always on — with the raw layer
([`RawMqttClient`](./raw-client)) available when you want it.

## Mental model

| MQTTnet | Pulse |
| --- | --- |
| `MqttFactory` / `MqttClientFactory` | `AddPulseMqttClient(...)` (DI) or `new ResilientMqttClient(...)` |
| `IMqttClient` (manual reconnect) | `RawMqttClient` (one connection, no reconnect) |
| `IManagedMqttClient` (auto reconnect + queue) | `ResilientMqttClient` (the default) |
| `MqttClientOptionsBuilder` | `ResilientMqttClientOptions` / `PulseMqttClientOptions` |
| `ApplicationMessageReceivedAsync` event | `Messages` channel or the [topic router](./routing) |
| `MqttApplicationMessageBuilder` | `MqttPublishPacket` or the [fluent `Publish(...)`](./fluent-api) |
| `MqttTopicFilterBuilder` | `MqttTopicFilter` |

## Creating and connecting a client

MQTTnet — build options, create the client, wire a reconnect loop yourself:

```csharp
var factory = new MqttFactory();
var client = factory.CreateMqttClient();
var options = new MqttClientOptionsBuilder()
    .WithTcpServer("broker.example.com", 1883)
    .WithClientId("my-service")
    .WithCredentials("user", "pass")
    .WithCleanSession(false)
    .Build();
await client.ConnectAsync(options, ct);
// + a DisconnectedAsync handler that retries...
```

Pulse — register it; it connects in the background and reconnects on its own:

```csharp
builder.Services.AddPulseMqttClient("devices", options =>
{
    options.Host = "broker.example.com";
    options.Port = 1883;
    options.ClientId = "my-service";
    options.Username = "user";
    options.Password = "pass";
    options.CleanStart = false;
});

// anywhere:
var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");
```

No host? `new ResilientMqttClient(new TcpTransportFactory(new TcpTransportOptions { Host = "..." }), options)`
then `await client.StartAsync(ct)`. There is **no `ConnectAsync` to await and no disconnect handler to
write** — watch [`State`](./lifecycle) if you want to observe the connection.

## Publishing

MQTTnet:

```csharp
var message = new MqttApplicationMessageBuilder()
    .WithTopic("sensors/boiler/temp")
    .WithPayload(payloadBytes)
    .WithQualityOfServiceLevel(MqttQualityOfServiceLevelExactlyOnce)
    .WithRetainFlag()
    .Build();
await client.PublishAsync(message, ct);
```

Pulse — a record, or the fluent builder:

```csharp
await client.PublishAsync(
    new MqttPublishPacket { Topic = "sensors/boiler/temp", Payload = payloadBytes, QualityOfService = MqttQualityOfService.ExactlyOnce, Retain = true },
    ct);

// or fluent / typed:
await client.Publish("sensors/boiler/temp").ExactlyOnce().WithRetain().WithPayload(reading).PublishAsync(ct);
```

Pulse returns a [`PublishOutcome`](./publishing#outcomes--no-silent-loss): `Delivered`, `Queued`
(offline), `DroppedOffline`, or `InFlight` — publishing while disconnected **queues** instead of
throwing, the same as MQTTnet's managed client `EnqueueAsync`, but you always learn which happened.

## Subscribing and receiving

MQTTnet routes everything through one event and you branch on the topic:

```csharp
client.ApplicationMessageReceivedAsync += async e =>
{
    if (e.ApplicationMessage.Topic.StartsWith("sensors/")) { /* ... */ }
};
await client.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("sensors/#").Build());
```

Pulse — subscribe, then either read the `Messages` channel or use the [router](./routing) with
templates and captured parameters:

```csharp
await client.SubscribeAsync([new MqttTopicFilter("sensors/+/temp") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }], ct);

using IDisposable route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/temp",
    (reading, message, token) => { Handle(message.Values["deviceId"], reading); return ValueTask.CompletedTask; });
```

Subscriptions are part of the [durable session](./resilience#sessions-and-re-subscription): Pulse
re-applies them automatically after a reconnect — there is no "resubscribe on reconnect" handler to
write.

## Retained will and birth

MQTTnet sets the will on the options builder; the "birth" announcement you publish yourself after
each connect:

```csharp
new MqttClientOptionsBuilder()
    .WithWillTopic("clients/me/status").WithWillPayload("offline").WithWillRetain()
```

Pulse pairs a retained will with an automatic [birth](./presence) message published on every
connection-up:

```csharp
options.Will = new PulseMqttWillOptions { Topic = "clients/me/status", Payload = "offline", Retain = true };
options.Birth = new PulseMqttBirthOptions { Topic = "clients/me/status", Payload = "online", Retain = true };
```

## The managed client

MQTTnet's `IManagedMqttClient` adds reconnect, a bounded message queue, and pluggable storage:

```csharp
var managed = factory.CreateManagedMqttClient();
await managed.StartAsync(new ManagedMqttClientOptionsBuilder()
    .WithClientOptions(clientOptions)
    .WithMaxPendingMessages(1000)
    .WithStorage(myStorage)
    .Build());
await managed.EnqueueAsync(message);
```

In Pulse this is the baseline `ResilientMqttClient`. The queue bound and overflow policy live on
[`OfflineQueue`](./resilience#the-offline-queue), and durable storage is the
[`Pulse.Mqtt.Storage.Sqlite`](/reference/packages) package:

```csharp
options.OfflineQueue = new OfflineQueueOptions { Capacity = 1000, Overflow = OverflowPolicy.DropOldest };
// durable across restarts:
options.MessageStore = new SqliteMessageStore("queue.db", options.OfflineQueue);
options.SessionStore = new SqliteSessionStore("session.db");
```

`EnqueueAsync` has no separate counterpart — `PublishAsync` queues automatically when offline.

## Request/response

MQTTnet has no built-in RPC; you correlate by hand with a response topic and a `TaskCompletionSource`.
Pulse ships it — see [Request and response](./request-response):

```csharp
StatusReply reply = await client.RequestAsync<StatusRequest, StatusReply>("devices/7/status", request, ct);
```

…including server-streamed responses via `RequestStreamAsync`.

## TLS and WebSocket

| MQTTnet | Pulse |
| --- | --- |
| `.WithTls()` | `options.UseTls = true` (DI) / a TLS `TcpTransportOptions` |
| `.WithWebSocketServer("wss://...")` | the `Pulse.Mqtt.Transport.WebSocket` package — see [Connecting](./connecting#websocket) |

## What changes for the better

- **No reconnect plumbing.** Drops, backoff, re-subscription, and offline flushing are built in and
  [observable](./observability), not something you assemble from event handlers.
- **No silent loss.** Every publish reports its [disposition](./publishing#outcomes--no-silent-loss);
  the offline queue counts drops; nothing disappears quietly.
- **Channels over events.** Received messages are a `ChannelReader` (or routed handlers), so
  backpressure and consumption are explicit rather than an `async void`-style event.
- **AOT-ready.** Source-generated JSON/MessagePack and a reflection-free core make
  [Native AOT](./native-aot) a first-class target.
