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
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

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

The route builder has subscribed terminals when one call should own both pieces:

```csharp
await using var route = await client.Route("orders/{id}")
    .AtLeastOnce()
    .HandleAsync(async (message, values, token) =>
    {
        await store.SaveAsync(values["id"], message.Payload, token);
    }, token);
```

`SubscribeAsync(template, qos, token)` is the concise form for a route-template broker
subscription. Use `MqttRouteTemplate.ToTopicFilter(...)` when MQTT 5 subscription options need
to be explicit:

```csharp
await client.SubscribeAsync([
    template.ToTopicFilter(
        MqttQualityOfService.AtLeastOnce,
        noLocal: true,
        retainHandling: MqttRetainHandling.DoNotSendAtSubscribe),
], token);
```

`NoLocal`, `RetainAsPublished`, `RetainHandling`, subscription identifiers, and subscription user
properties are MQTT 5 subscription features. On MQTT 3.1.1, only the topic filter and maximum QoS
are encoded.

Typed handlers deserialize through the configured [serializer](./typed-messaging):

```csharp
var template = MqttRouteTemplate.Parse("sensors/{deviceId}/telemetry");
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

using var route = client.RegisterRoute<TelemetryReading>(
    template,
    (reading, message, token) => Handle(reading, message.Values["deviceId"]));
```

## Streams

Prefer pull over callbacks where it reads better:

```csharp
var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync(template, token);

await using MqttRouteStream stream = client.OpenRouteStream(template);
await foreach (MqttRoutedMessage routed in stream.ReadAllAsync(token))
{
    Process(routed.Values["deviceId"], routed.Message);
}
```

## Dataflow source blocks

Add `Pulse.Mqtt.Dataflow` when a route should feed a bounded pipeline:

```csharp
using Pulse.Mqtt.Dataflow;
using System.Threading.Tasks.Dataflow;

var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync(template, token);

await using var source = client.ToRouteSourceBlock(
    template,
    sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 128 },
    cancellationToken: token);

using var link = source.LinkTo(
    new ActionBlock<MqttRoutedMessage>(
        routed => Process(routed.Values["deviceId"], routed.Message),
        new ExecutionDataflowBlockOptions { BoundedCapacity = 128 }),
    new DataflowLinkOptions { PropagateCompletion = true });
```

`ToRouteSourceBlock` is a local routing adapter only. It does not subscribe to the broker; call
`SubscribeAsync` first, or use an existing fluent route helper when you want subscription and
local route ownership together. For event-style consumers, use `DataflowBlock.AsObservable(source)`.

## Delivery modes

The default raw stream and routes use automatic acknowledgement: inbound QoS 1/2 publishes are
acknowledged after Pulse accepts them into local routing. `SubscribeAsync` does not change that;
it only controls broker subscription.

Use manual acknowledgement when the broker must not receive PUBACK/PUBREC until application work
has completed. Manual delivery is a route-local choice:

```csharp
await using var route = await client.Route("jobs/{jobId}")
    .AtLeastOnce()
    .ManualAcknowledgement()
    .HandleAsync(async (routed, token) =>
    {
        await RunJobAsync(routed.Values["jobId"], routed.Message, token);
        await routed.AcknowledgeAsync(token);
    }, token);
```

Use the low-level manual handler API when broker subscription ownership is separate:

```csharp
var template = MqttRouteTemplate.Parse("jobs/{jobId}");
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

using var route = client.RegisterManualAcknowledgementRoute(
    template,
    async (routed, token) =>
    {
        await RunJobAsync(routed.Values["jobId"], routed.Message, token);
        await routed.AcknowledgeAsync(token);
    });
```

For pull consumers, use either the subscribed fluent terminal or the low-level stream:

```csharp
await using var stream = await client.Route("jobs/{jobId}")
    .AtLeastOnce()
    .ManualAcknowledgement()
    .StreamAsync(token);

await foreach (MqttAcknowledgedRoutedMessage routed in stream.ReadAllAsync(token))
{
    await RunJobAsync(routed.Values["jobId"], routed.Message, token);
    await routed.AcknowledgeAsync(token);
}
```

The equivalent low-level stream keeps broker subscription separate:

```csharp
var template = MqttRouteTemplate.Parse("jobs/{jobId}");
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

await using MqttAcknowledgedRouteStream stream = client.OpenAcknowledgedRouteStream(template);
await foreach (MqttAcknowledgedRoutedMessage routed in stream.ReadAllAsync(token))
{
    try
    {
        await RunJobAsync(routed.Values["jobId"], routed.Message, token);
        await routed.AcknowledgeAsync(token);
    }
    catch (Exception error)
    {
        if (routed.CanReject)
        {
            await routed.RejectAsync(MqttReasonCode.UnspecifiedError, error.Message, token);
        }
        else
        {
            throw;
        }
    }
}
```

Manual routes are single-owner routes: the first matching manual route receives the message and
owns its `AcknowledgeAsync` / `RejectAsync` call. If no manual route matches, Pulse falls back to
the normal automatic raw message stream and automatic routes.

`RejectAsync` is available only when `CanReject` is true: MQTT 5 QoS 1/2 deliveries can carry
negative publish acknowledgement reason codes. MQTT 3.1.1 and QoS 0 deliveries cannot, so
`RejectAsync` throws `NotSupportedException` and leaves the message unacknowledged.

Manual acknowledgement routes are lossless-only: `MqttRouteOptions.Overflow` must be `Wait`. Lossy
overflow modes are rejected because dropping a queued message would also drop the only pending
protocol acknowledgement context.

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
