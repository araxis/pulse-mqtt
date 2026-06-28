# Protobuf serializer package

Package: `Pulse.Mqtt.Serialization.Protobuf`

Use this package when typed MQTT payloads are generated Protocol Buffers messages and
deserialization should use explicit parser registration instead of reflection-based discovery.

## Install

```shell
dotnet add package Pulse.Mqtt.Serialization.Protobuf
```

## Register message parsers

Register every generated message type that the client must deserialize:

```csharp
using Pulse.Mqtt.Serialization.Protobuf;

var registry = ProtobufMessageRegistry.Create(registry =>
{
    registry.Add(TelemetryReading.Parser);
    registry.Add(StatusRequest.Parser);
    registry.Add(StatusReply.Parser);
});

var serializer = new ProtobufMqttSerializer(registry);
```

Parser registration is explicit by design. It keeps the package trim/Native AOT friendly and
avoids scanning assemblies at runtime.

## Configure with dependency injection

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSerializer(_ => new ProtobufMqttSerializer(registry));
```

## Configure directly

```csharp
var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "telemetry-worker" },
    Serializer = new ProtobufMqttSerializer(registry),
};
```

## Customize metadata

The default metadata is:

| Field | Value |
| --- | --- |
| `ContentType` | `application/x-protobuf` |
| `PayloadFormatIndicator` | `Unspecified` |

Use `ProtobufMqttSerializerOptions` when the application needs a different content type:

```csharp
var serializer = new ProtobufMqttSerializer(new ProtobufMqttSerializerOptions
{
    Registry = registry,
    ContentType = "application/vnd.example.telemetry+protobuf",
});
```

## Use typed APIs

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new TelemetryReading { DeviceId = "device-7", Value = 21.5 },
    MqttQualityOfService.AtLeastOnce,
    cancellationToken: ct);

var template = MqttRouteTemplate.Parse("telemetry/{deviceId}");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);

using var route = client.RegisterRoute<TelemetryReading>(
    template,
    (reading, message, token) => HandleAsync(reading, message.Values["deviceId"], token));
```

## Requirements and limitations

- Values passed to typed publish APIs must implement the generated-message interfaces.
- Deserialization requires a registered parser for the target type.
- Invalid payloads and missing parsers throw `MqttException`.
- The package does not infer message types from MQTT topics or content types.

## Related docs

- [Typed messaging](/guide/typed-messaging)
- [Native AOT](/guide/native-aot)
- [Serializer package overview](./serializers)
