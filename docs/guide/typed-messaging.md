# Typed messaging

Publish and consume objects instead of byte arrays. One serializer is configured per client;
every typed API uses it.

## Configure a serializer

The JSON implementation (package `Pulse.Mqtt.Serialization.Json`) is built on source-generated
`System.Text.Json` — reflection-free and Native AOT safe. Hand it your `JsonSerializerContext`:

```csharp
[JsonSerializable(typeof(TelemetryReading))]
[JsonSerializable(typeof(StatusRequest))]
[JsonSerializable(typeof(StatusReply))]
public sealed partial class AppJsonContext : JsonSerializerContext;
```

```csharp
// With dependency injection:
.UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default))

// Direct construction:
new ResilientMqttClientOptions { Serializer = new JsonMqttSerializer(AppJsonContext.Default), ... }
```

Without a serializer configured, the typed APIs throw `InvalidOperationException` — early and
explicit, not on a background thread later.

## Publish

```csharp
await client.PublishAsync("sensors/boiler-1/telemetry", reading, MqttQualityOfService.AtLeastOnce);
```

The serializer stamps wire metadata so consumers (including non-.NET ones) know what they got:
`ContentType` (`application/json`) and `PayloadFormatIndicator` (`Utf8`).

## Consume

```csharp
using var route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    (reading, message, token) =>
    {
        // reading       — the deserialized payload
        // message       — the raw routed message: captured values, QoS, properties
        return Handle(reading, message.Values["deviceId"]);
    });
```

Everything from [routing](./routing) applies: bounded queues, overflow policies, concurrency,
fault isolation.

## Request and response

The typed [RPC APIs](./request-response) serialize requests and responses through the same
serializer on both sides.

## Bring your own format

`IMqttSerializer` is one small interface:

```csharp
public sealed class MessagePackMqttSerializer : IMqttSerializer
{
    public string ContentType => "application/x-msgpack";
    public MqttPayloadFormatIndicator PayloadFormat => MqttPayloadFormatIndicator.Unspecified;

    public ReadOnlyMemory<byte> Serialize<T>(T value) => MessagePackSerializer.Serialize(value);
    public T Deserialize<T>(ReadOnlyMemory<byte> payload) => MessagePackSerializer.Deserialize<T>(payload);
}
```

```csharp
.UseSerializer(_ => new MessagePackMqttSerializer())
```

Every typed API — publish, routes, streams, RPC — picks it up. Nothing else changes.

::: tip Mixed payloads on one client
The serializer is per client. For genuinely mixed formats, use the raw
`MqttPublishPacket` APIs alongside typed ones, or register a second named client.
:::
