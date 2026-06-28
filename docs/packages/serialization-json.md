# JSON serializer package

Package: `Pulse.Mqtt.Serialization.Json`

Use this package when typed MQTT payloads should be UTF-8 JSON and remain trim/Native AOT
friendly through `System.Text.Json` source generation.

## Install

```shell
dotnet add package Pulse.Mqtt.Serialization.Json
```

## Define generated metadata

Create a source-generation context that includes every typed payload used by publish, route,
stream, and request/response APIs:

```csharp
using System.Text.Json.Serialization;

[JsonSerializable(typeof(TelemetryReading))]
[JsonSerializable(typeof(StatusRequest))]
[JsonSerializable(typeof(StatusReply))]
public sealed partial class AppJsonContext : JsonSerializerContext;
```

Use generated metadata instead of reflection-based serialization. That keeps the serializer
predictable under trimming and Native AOT.

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Serialization.Json;

builder.Services
    .AddPulseMqttClient("devices", options =>
    {
        options.Host = "broker.example.com";
        options.ClientId = "device-worker";
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default));
```

Each named client has one serializer. Register another named client when different topics need a
different payload format.

## Configure directly

```csharp
await using var client = new ResilientMqttClient(
    transportFactory,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "device-worker" },
        Serializer = new JsonMqttSerializer(AppJsonContext.Default),
    });
```

## Wire metadata

Typed publishes created with this serializer stamp:

| Field | Value |
| --- | --- |
| `ContentType` | `application/json` |
| `PayloadFormatIndicator` | `Utf8` |

The metadata is attached only to typed publishes. Raw `MqttPublishPacket` sends exactly what the
caller provides.

## Use typed APIs

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new TelemetryReading("device-7", 21.5),
    MqttQualityOfService.AtLeastOnce,
    cancellationToken: ct);

var template = MqttRouteTemplate.Parse("telemetry/{deviceId}");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);

using var route = client.RegisterRoute<TelemetryReading>(
    template,
    (reading, message, token) => HandleAsync(reading, message.Values["deviceId"], token));
```

The same serializer is used by typed request/response and typed route streams.

## Failure behavior

- If no serializer is configured, typed APIs throw `InvalidOperationException`.
- If JSON cannot deserialize to the target type, the serializer throws `MqttException`.
- If a type is missing from the source-generation context, serialization fails with the normal
  `System.Text.Json` metadata error.

## Related docs

- [Typed messaging](/guide/typed-messaging)
- [Native AOT](/guide/native-aot)
- [Serializer package overview](./serializers)
