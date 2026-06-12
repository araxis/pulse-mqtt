# Routing

Routing turns "one stream of messages" into "the right handler with the right data" — the same
jump minimal APIs made over raw HTTP handling.

## Route templates

A template is a topic filter with named captures:

| Template | Subscribes | Captures |
| --- | --- | --- |
| `sensors/{deviceId}/temp` | `sensors/+/temp` | `deviceId` |
| `plant/{line}/sensors/{kind}` | `plant/+/sensors/+` | `line`, `kind` |
| `logs/{app}/{**rest}` | `logs/+/#` | `app`, and `rest` captures the remaining levels |

Matching is span-based and allocation-free until values are captured (~56 ns with two
captures).

## Handlers

```csharp
using IDisposable route = await client.OnAsync(
    "sensors/{deviceId}/temp",
    async (message, values, token) =>
    {
        await store.SaveAsync(values["deviceId"], message.Payload, token);
    },
    new MqttRouteOptions { MaxConcurrency = 4 });
```

Registering a route also subscribes its filter (at
`MqttRouteOptions.SubscriptionQualityOfService`, default QoS 1). Disposing the registration
removes the route; the broker subscription stays until `UnsubscribeAsync` — other routes may
share the same filter.

Typed handlers deserialize through the configured [serializer](./typed-messaging):

```csharp
using var route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    (reading, message, token) => Handle(reading, message.Values["deviceId"]));
```

## Streams

Prefer pull over callbacks where it reads better:

```csharp
await using MqttRouteStream stream = await client.OpenStreamAsync("sensors/{deviceId}/temp");
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
| `SubscriptionQualityOfService` | `AtLeastOnce` | QoS requested for the route's filter |

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
