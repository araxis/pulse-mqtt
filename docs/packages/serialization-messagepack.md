# MessagePack serializer package

Package: `Pulse.Mqtt.Serialization.MessagePack`

Use this package when typed MQTT payloads should be compact binary messages and the application
already owns MessagePack contracts or wants smaller payloads than JSON.

## Install

```shell
dotnet add package Pulse.Mqtt.Serialization.MessagePack
```

## Configure generated resolvers

Build `MessagePackSerializerOptions` with generated resolvers for the message types you publish
and consume. The exact resolver setup depends on how the application generates MessagePack
metadata, but the serializer expects the finished options object:

```csharp
using MessagePack;
using MessagePack.Resolvers;
using Pulse.Mqtt.Serialization.MessagePack;

[MessagePackObject]
public sealed record TelemetryReading(
    [property: Key(0)] string DeviceId,
    [property: Key(1)] double Value);

var messagePackOptions = MessagePackSerializerOptions.Standard.WithResolver(
    CompositeResolver.Create(
        GeneratedMessagePackResolver.Instance,
        StandardResolver.Instance));

var serializer = new MessagePackMqttSerializer(messagePackOptions);
```

Prefer generated resolvers for trimming and Native AOT. Avoid contractless or reflection-heavy
configuration in applications that publish with trimming enabled.

## Configure with dependency injection

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSerializer(_ => new MessagePackMqttSerializer(messagePackOptions));
```

## Configure directly

```csharp
var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "telemetry-worker" },
    Serializer = new MessagePackMqttSerializer(messagePackOptions),
};
```

## Wire metadata

Typed publishes created with this serializer stamp:

| Field | Value |
| --- | --- |
| `ContentType` | `application/x-msgpack` |
| `PayloadFormatIndicator` | `Unspecified` |

`Unspecified` is intentional because the payload is binary, not UTF-8 text.

## Use typed APIs

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new TelemetryReading("device-7", 21.5),
    MqttQualityOfService.AtLeastOnce,
    cancellationToken: ct);
```

Consumers must use the same MessagePack contracts and resolver configuration for typed
deserialization.

## Failure behavior

- If no serializer is configured, typed APIs throw `InvalidOperationException`.
- Invalid MessagePack payloads are surfaced as `MqttException`.
- Resolver or contract mismatches fail during serialization/deserialization rather than being
  silently ignored.

## Related docs

- [Typed messaging](/guide/typed-messaging)
- [Native AOT](/guide/native-aot)
- [Serializer package overview](./serializers)
