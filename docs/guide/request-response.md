# Request and response

MQTT 5 carries a **response topic** and **correlation data** on every message, which makes RPC
a first-class pattern instead of a hand-rolled convention. Pulse wires both sides.

## Caller

```csharp
StatusReply reply = await client.RequestAsync<StatusRequest, StatusReply>(
    "devices/boiler-1/status",
    new StatusRequest("dashboard"),
    new MqttRequestOptions { Timeout = TimeSpan.FromSeconds(10) },
    token);
```

What happens underneath:

1. On first use, the client subscribes once to its private reply filter:
   `pulse-rpc/<clientId>/+`.
2. The request is published with `ResponseTopic = pulse-rpc/<clientId>/<correlation>` and
   unique correlation data.
3. The matching reply resolves the call. Concurrent requests never cross — correlation data
   pairs each reply with its caller.

| `MqttRequestOptions` | Default | Meaning |
| --- | --- | --- |
| `Timeout` | 30 s | How long to wait for the reply before failing with `MqttException` |
| `QualityOfService` | `AtLeastOnce` | QoS of the request publish |

A raw overload takes and returns `MqttPublishPacket` for untyped payloads:

```csharp
MqttPublishPacket reply = await client.RequestAsync(
    new MqttPublishPacket { Topic = "devices/boiler-1/status", Payload = bytes }, options, token);
```

::: warning Requests need a clean packet
Leave `ResponseTopic` and `CorrelationData` unset on the request — the client manages both and
rejects packets that pre-set them.
:::

## Responder

```csharp
var template = MqttRouteTemplate.Parse("devices/{deviceId}/status");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], token);

using IDisposable responder = client.RegisterRequestHandler<StatusRequest, StatusReply>(
    template,
    async (request, message, token) =>
    {
        var deviceId = message.Values["deviceId"];
        return new StatusReply(deviceId, await ProbeAsync(deviceId, token));
    });
```

The responder is a [route](./routing): subscribe the broker filter with `SubscribeAsync`, then
register the local responder with `RegisterRequestHandler`. Templates, captured values, bounded
queues, and fault isolation all apply. For each request it:

1. Deserializes the payload to `TRequest`.
2. Runs your handler.
3. Publishes the `TResponse` to the request's response topic, echoing the correlation data, at
   the request's QoS, stamped with the serializer's content type.

Messages **without** a response topic are ignored rather than failed — plain publishes to the
same topic stay harmless.

## Streaming responses

When one request has *many* answers — a query that pages, a job that reports progress — stream
them. The caller consumes an `IAsyncEnumerable<TResponse>` that ends when the responder publishes
an end-of-stream marker, the idle timeout elapses between items, or the enumeration is cancelled:

```csharp
await foreach (var row in client.RequestStreamAsync<Query, Row>("reports/run", query, cancellationToken: token))
{
    Render(row);
    if (EnoughForNow()) break;   // abandoning early cleans up; later responses are dropped
}
```

The responder yields its results and Pulse publishes each one, then the marker, automatically:

```csharp
var template = MqttRouteTemplate.Parse("reports/run");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], token);

using IDisposable responder = client.RegisterRequestStreamHandler<Query, Row>(
    template,
    (query, message, token) => RunReportAsync(query, token));   // returns IAsyncEnumerable<Row>
```

Each stream buffers up to `MqttRequestStreamOptions.Capacity` responses. Delivery is non-blocking,
so a slow consumer never holds up other requests or the client's inbound pipeline — but a consumer
that falls *behind* its capacity fails that one stream with an `MqttException` rather than blocking
or dropping silently. Size `Capacity` for your consumer's pace, and use `IdleTimeout` to bound how
long an idle stream waits for the next response before giving up.

## Failure behavior

- **No responder / responder offline**: the caller times out with `MqttException`. Choose
  `Timeout` for your latency budget rather than relying on the 30 s default.
- **Handler throws**: the route logs and isolates the fault; the caller times out. Prefer
  encoding domain errors *into* `TResponse` so callers get answers, not timeouts.
- **Client offline**: request publishes follow normal [offline behavior](./resilience) — at
  QoS 1 they queue and flush on reconnect; the timeout clock keeps running.

## Scaling responders

Run several instances of a responder service and subscribe the route through a
[shared subscription](./subscribing#shared-subscriptions-mqtt-5) so the broker load-balances
requests across the group. Replies still find the right caller — the response topic targets
one specific client.

Both sides need a [serializer](./typed-messaging) configured for the typed overloads.
