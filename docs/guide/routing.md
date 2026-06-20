# Routing

Routing turns "one stream of messages" into "the right handler with the right data" — the same
jump minimal APIs made over raw HTTP handling.

## Route templates

A template is a topic filter with named captures:

| Template | Broker filter | Captures |
| --- | --- | --- |
| `sensors/{deviceId}/temp` | `sensors/+/temp` | `deviceId` |
| `plant/{line}/sensors/{kind}` | `plant/+/sensors/+` | `line`, `kind` |
| `logs/{app}/{**rest}` | `logs/+/#` | `app`, and `rest` captures the remaining levels |

Matching is span-based and allocation-free until values are captured (~56 ns with two
captures).

## Handlers

For the common "subscribe and handle this route" case, use endpoint-style `OnAsync`:

```csharp
await using var route = await client.OnAsync(
    "sensors/{deviceId}/temp",
    MqttQualityOfService.AtLeastOnce,
    async (message, values, token) =>
    {
        await store.SaveAsync(values["deviceId"], message.Payload, token);
    },
    token);
```

`OnAsync` is shorthand, not a second routing model. It registers the local route, subscribes
the route's broker filter, and returns a handle whose async disposal removes both.

Use the explicit form when you want separate broker subscription ownership or advanced route
queue/concurrency settings:

```csharp
var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], token);

using IDisposable route = client.RegisterRoute(
    template,
    async (message, values, token) =>
    {
        await store.SaveAsync(values["deviceId"], message.Payload, token);
    },
    new MqttRouteOptions { MaxConcurrency = 4 });
```

`SubscribeAsync` owns broker delivery. `RegisterRoute` owns local dispatch. Disposing the route
registration removes the local handler only; use `UnsubscribeAsync` when the broker should stop
delivering that filter.

`MqttRouteTemplate.ToTopicFilter(...)` keeps the subscription side readable while still making
MQTT 5 subscription options explicit:

```csharp
await client.SubscribeAsync([
    template.ToTopicFilter(
        MqttQualityOfService.AtLeastOnce,
        noLocal: true,
        retainHandling: MqttRetainHandling.DoNotSendAtSubscribe),
], token);
```

Typed handlers deserialize through the configured [serializer](./typed-messaging):

```csharp
var template = MqttRouteTemplate.Parse("sensors/{deviceId}/telemetry");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], token);

using var route = client.RegisterRoute<TelemetryReading>(
    template,
    (reading, message, token) => Handle(reading, message.Values["deviceId"]));
```

## Streams

Prefer pull over callbacks where it reads better:

```csharp
var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync([template.ToTopicFilter()], token);

await using MqttRouteStream stream = client.OpenRouteStream(template);
await foreach (MqttRoutedMessage routed in stream.ReadAllAsync(token))
{
    Process(routed.Values["deviceId"], routed.Message);
}
```

## Isolation and backpressure

Every route owns a **bounded queue** and a dispatcher:

| `MqttRouteOptions` | Default | Meaning |
| --- | --- | --- |
| `Capacity` | 64 | Bound of the route's queue |
| `Overflow` | `Wait` | What happens when it is full |
| `MaxConcurrency` | 1 | Concurrent handler invocations; 1 preserves per-route order |

Overflow choices:

- **`Wait`** — lossless; once this route's queue is full, backpressure reaches the shared
  dispatcher (and eventually the socket). Right for must-not-lose data.
- **`DropOldest`** — keep the newest readings; a slow route never affects the others. Right
  for telemetry where only the latest matters.
- **`DropNewest`** — keep what is already queued; new messages are discarded while full.

A handler that **throws** faults only its own route — the failure is logged
(`RouteHandlerFaulted`) and other routes plus the connection keep running. A faulted route
stops consuming; fix and re-register.

## Multiple matches

A message matching several routes is dispatched to **each** of them independently — separate
queues, separate failures, separate pace.

## When not to route

Gateway-style code that forwards everything regardless of topic is better served by the raw
[`client.Messages` stream](./subscribing#consuming-the-raw-message-stream). Pick one model per
client: the router consumes the same underlying stream.
