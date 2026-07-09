# Pulse.Mqtt.Client

The high-level resilient MQTT client for .NET. It wraps the core protocol layer with reconnect supervision, re-subscription, bounded offline publishing, route templates, typed payloads, request/reply, and route-local delivery modes.

## Install

```shell
dotnet add package Pulse.Mqtt.Client
```

Add optional packages for hosting, serializers, durable storage, alternate transports, or analyzers as needed.

## Connect

```csharp
var transport = new TcpTransportFactory(new TcpTransportOptions
{
    Host = "broker.example.com",
    Port = 8883,
    UseTls = true,
});

await using var client = new ResilientMqttClient(
    transport,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "service-1" },
    });

await client.ConnectAsync(cancellationToken);
await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(10), cancellationToken);
```

## Route messages

`SubscribeAsync` controls broker delivery. Routes control local dispatch.

```csharp
var template = MqttRouteTemplate.Parse("orders/{id}");

await client.SubscribeAsync(
    [template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)],
    cancellationToken);

using var route = client.RegisterRoute(template, async (message, values, ct) =>
{
    var orderId = values["id"];
    await ProcessOrderAsync(orderId, message.Payload, ct);
});
```

## Manual acknowledgement per route

Automatic acknowledgement is the default. Use manual acknowledgement only on routes that must finish local work before the broker is acknowledged.

```csharp
await using var route = await client.Route("orders/{id}")
    .AtLeastOnce()
    .ManualAcknowledgement()
    .HandleAsync(async (message, ct) =>
    {
        await PersistAsync(message.Message, ct);
        await message.AcknowledgeAsync(ct);
    }, cancellationToken);
```

## Typed messaging

Configure one serializer, then publish and consume typed payloads.

```csharp
await client.PublishAsync(
    "telemetry/device-7",
    new Reading("device-7", 21.5),
    MqttQualityOfService.AtLeastOnce,
    cancellationToken);
```

Serializer packages:

- `Pulse.Mqtt.Serialization.Json`
- `Pulse.Mqtt.Serialization.MessagePack`
- `Pulse.Mqtt.Serialization.Protobuf`

Full docs: https://araxis.github.io/pulse-mqtt/guide/quick-start
