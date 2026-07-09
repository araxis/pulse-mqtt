# Pulse.Mqtt.Serialization.MessagePack

MessagePack payload serialization for typed Pulse MQTT APIs. Use it when payloads should be compact binary messages and the application already owns MessagePack contracts.

## Install

```shell
dotnet add package Pulse.Mqtt.Serialization.MessagePack
```

## Configure serializer options

```csharp
using MessagePack;
using MessagePack.Resolvers;
using Pulse.Mqtt.Serialization.MessagePack;

[MessagePackObject]
public sealed record Reading(
    [property: Key(0)] string DeviceId,
    [property: Key(1)] double Value);

var messagePackOptions = MessagePackSerializerOptions.Standard.WithResolver(
    CompositeResolver.Create(
        GeneratedMessagePackResolver.Instance,
        StandardResolver.Instance));
```

Prefer generated resolvers in trimmed or Native AOT applications.

## Configure the client

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSerializer(_ => new MessagePackMqttSerializer(messagePackOptions));
```

## Use typed APIs

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new Reading("device-7", 21.5),
    MqttQualityOfService.AtLeastOnce,
    cancellationToken);
```

Typed MessagePack publishes stamp `ContentType = "application/x-msgpack"` and binary payload metadata. Consumers must use matching MessagePack contracts.

Full docs: https://araxis.github.io/pulse-mqtt/packages/serialization-messagepack
