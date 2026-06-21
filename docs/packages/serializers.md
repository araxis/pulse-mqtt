# Serializer packages

Typed publish, route, stream, and request/response APIs use one serializer per client.

## JSON

Package: `Pulse.Mqtt.Serialization.Json`

```shell
dotnet add package Pulse.Mqtt.Serialization.Json
```

```csharp
[JsonSerializable(typeof(TelemetryReading))]
public sealed partial class AppJsonContext : JsonSerializerContext;

.UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default))
```

Use source-generated metadata for trimming and Native AOT.

## MessagePack

Package: `Pulse.Mqtt.Serialization.MessagePack`

```shell
dotnet add package Pulse.Mqtt.Serialization.MessagePack
```

```csharp
var serializer = new MessagePackMqttSerializer(messagePackOptions);
```

Build `MessagePackSerializerOptions` with generated resolvers for trimming and Native AOT.

## Protocol Buffers

Package: `Pulse.Mqtt.Serialization.Protobuf`

```shell
dotnet add package Pulse.Mqtt.Serialization.Protobuf
```

```csharp
var registry = ProtobufMessageRegistry.Create(registry =>
{
    registry.Add(TelemetryReading.Parser);
    registry.Add(StatusReply.Parser);
});

var serializer = new ProtobufMqttSerializer(registry);
```

Deserialization uses explicit parser registration. Missing parsers and invalid payloads throw
`MqttException`.

See [Typed messaging](/guide/typed-messaging) for publish, route, stream, and request/response
examples.
