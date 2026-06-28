# Typed messaging

Publish and consume objects instead of byte arrays. One serializer is configured per client;
every typed API uses it.

## Configure a serializer

The JSON implementation (package
[`Pulse.Mqtt.Serialization.Json`](/packages/serialization-json)) is built on source-generated
`System.Text.Json`: reflection-free and Native AOT safe. Hand it your `JsonSerializerContext`:

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
await using var route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    MqttQualityOfService.AtLeastOnce,
    (reading, message, token) =>
    {
        // reading       — the deserialized payload
        // message       — the raw routed message: captured values, QoS, properties
        return Handle(reading, message.Values["deviceId"]);
    },
    token);
```

Everything from [routing](./routing) applies: bounded queues, overflow policies, concurrency,
fault isolation.

## Request and response

The typed [RPC APIs](./request-response) serialize requests and responses through the same
serializer on both sides.

## MessagePack

For a compact binary wire format, add the
[`Pulse.Mqtt.Serialization.MessagePack`](/packages/serialization-messagepack) package. Like the
JSON serializer it stays reflection-free: hand it `MessagePackSerializerOptions` built from a
[source-generated resolver](https://github.com/MessagePack-CSharp/MessagePack-CSharp#aot-code-generation-to-support-unityxamarin-and-native-aot)
(annotate your types with `[MessagePackObject]`), so there is no dynamic codegen and it is Native
AOT safe:

```csharp
var options = MessagePackSerializerOptions.Standard.WithResolver(
    CompositeResolver.Create(GeneratedMessagePackResolver.Instance, StandardResolver.Instance));

new ResilientMqttClientOptions { Serializer = new MessagePackMqttSerializer(options), ... }
```

It stamps `ContentType` (`application/x-msgpack`) and a binary `PayloadFormatIndicator`
(`Unspecified`). The payloads are materially smaller than JSON.

## Protobuf

For generated Protocol Buffers messages, add the
[`Pulse.Mqtt.Serialization.Protobuf`](/packages/serialization-protobuf) package:

```shell
dotnet add package Pulse.Mqtt.Serialization.Protobuf
```

Register the generated parsers explicitly. This keeps deserialization reflection-free and makes
the message types visible to trim and Native AOT analysis:

```csharp
var protobufRegistry = ProtobufMessageRegistry.Create(registry =>
{
    registry.Add(TelemetryReading.Parser);
    registry.Add(StatusRequest.Parser);
    registry.Add(StatusReply.Parser);
});

var serializer = new ProtobufMqttSerializer(protobufRegistry);
```

Use it exactly like the other serializers:

```csharp
new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "telemetry-service" },
    Serializer = serializer,
};
```

```csharp
await client.PublishAsync(
    "telemetry/1",
    new TelemetryReading { DeviceId = "dev-1", Value = 21.5 },
    cancellationToken: ct);

var template = MqttRouteTemplate.Parse("telemetry/{id}");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);

using var route = client.RegisterRoute<TelemetryReading>(
    template,
    (reading, message, token) => Handle(reading, message.Values["id"]));
```

It stamps `ContentType` (`application/x-protobuf`) and a binary `PayloadFormatIndicator`
(`Unspecified`).

::: warning Protobuf requirements
`ProtobufMqttSerializer` accepts generated message instances only. Deserialization requires a
registered parser for the target type; missing parsers and invalid payloads throw `MqttException`.
:::

## Bring your own format

`IMqttSerializer` is one small interface — `ContentType`, `PayloadFormat`, `Serialize<T>`,
`Deserialize<T>`. Implement it for CBOR, Avro, or anything else, then:

```csharp
.UseSerializer(_ => new MyFormatSerializer())
```

Every typed API — publish, routes, streams, RPC — picks it up. Nothing else changes.

::: tip Mixed payloads on one client
The serializer is per client. For genuinely mixed formats, use the raw
`MqttPublishPacket` APIs alongside typed ones, or register a second named client.
:::
