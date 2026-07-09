# Pulse.Mqtt.Serialization.Json

`System.Text.Json` payload serialization for typed Pulse MQTT publishes, routes, streams, and request/reply APIs. The serializer is designed for source-generated metadata, trimming, and Native AOT.

## Install

```shell
dotnet add package Pulse.Mqtt.Serialization.Json
```

## Define JSON metadata

```csharp
using System.Text.Json.Serialization;

[JsonSerializable(typeof(Reading))]
[JsonSerializable(typeof(StatusRequest))]
[JsonSerializable(typeof(StatusReply))]
public sealed partial class AppJsonContext : JsonSerializerContext;
```

## Configure the client

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", options =>
    {
        options.Host = "broker.example.com";
        options.ClientId = "telemetry-worker";
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default));
```

Direct construction uses the same serializer:

```csharp
var options = new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "telemetry-worker" },
    Serializer = new JsonMqttSerializer(AppJsonContext.Default),
};
```

## Use typed APIs

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new Reading("device-7", 21.5),
    MqttQualityOfService.AtLeastOnce,
    cancellationToken);
```

Typed JSON publishes stamp `ContentType = "application/json"` and `PayloadFormatIndicator = Utf8`. Raw `MqttPublishPacket` sends exactly what the caller provides.

Full docs: https://araxis.github.io/pulse-mqtt/packages/serialization-json
