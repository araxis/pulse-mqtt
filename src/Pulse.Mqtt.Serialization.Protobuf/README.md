# Pulse.Mqtt.Serialization.Protobuf

Protocol Buffers payload serialization for typed Pulse MQTT APIs. Deserialization uses explicit parser registration instead of assembly scanning.

## Install

```shell
dotnet add package Pulse.Mqtt.Serialization.Protobuf
```

## Register message parsers

```csharp
using Pulse.Mqtt.Serialization.Protobuf;

var registry = ProtobufMessageRegistry.Create(registry =>
{
    registry.Add(Reading.Parser);
    registry.Add(StatusRequest.Parser);
    registry.Add(StatusReply.Parser);
});

var serializer = new ProtobufMqttSerializer(registry);
```

## Configure the client

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSerializer(_ => serializer);
```

## Use typed APIs

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new Reading { DeviceId = "device-7", Value = 21.5 },
    MqttQualityOfService.AtLeastOnce,
    cancellationToken);
```

Typed protobuf publishes default to `ContentType = "application/x-protobuf"`. Register every message type that the client must deserialize.

Full docs: https://araxis.github.io/pulse-mqtt/packages/serialization-protobuf
