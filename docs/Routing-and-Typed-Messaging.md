# Routing and typed messaging

## Route templates

A route template is an MQTT topic filter with named captures: `sensors/{deviceId}/temp`
subscribes to `sensors/+/temp` and hands each handler the captured `deviceId`. Multi-level
tails work too: `logs/{app}/{**rest}` maps to `logs/+/#`.

## Handlers

```csharp
using var registration = await client.OnAsync(
    "sensors/{deviceId}/temp",
    (message, values, token) =>
    {
        Console.WriteLine($"{values["deviceId"]}: {message.Payload.Length} bytes");
        return ValueTask.CompletedTask;
    });
```

Each route gets its own bounded queue and configurable concurrency through
`MqttRouteOptions`; a handler that throws faults only its route, never the connection.
Dispose the registration to remove the route — the subscription itself stays until
`UnsubscribeAsync`.

## Streams

Prefer pull over callbacks where it reads better:

```csharp
await using var stream = await client.OpenStreamAsync("sensors/{deviceId}/temp");
await foreach (var routed in stream.ReadAllAsync(token))
{
    Process(routed.Values["deviceId"], routed.Message);
}
```

## Typed messages

Configure a serializer once (`UseSerializer`, or `ResilientMqttClientOptions.Serializer`), then
publish and consume objects. The JSON implementation is source-generated and AOT-safe — hand it
your `JsonSerializerContext`:

```csharp
var serializer = new JsonMqttSerializer(AppJsonContext.Default);
```

```csharp
await client.PublishAsync("sensors/boiler-1/telemetry", reading, MqttQualityOfService.AtLeastOnce);

using var route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    (reading, message, token) => Handle(reading, message.Values["deviceId"]));
```

The serializer is a swap point (`IMqttSerializer`): implement it once for MessagePack,
Protobuf, or anything else, and every typed API picks it up.

## Backpressure

Route queues are bounded (`MqttRouteOptions.QueueCapacity`). When a queue is full the overflow
policy decides: wait (default), drop oldest, or drop newest. The client's inbound channel is
bounded as well, so a slow consumer slows the reader instead of growing the heap.
