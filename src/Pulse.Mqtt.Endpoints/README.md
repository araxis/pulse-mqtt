# Pulse.Mqtt.Endpoints

Minimal-API-style MQTT endpoints. `MapMqtt` subscribes a route template, registers a handler, and gives each message a scoped endpoint context.

## Install

```shell
dotnet add package Pulse.Mqtt.Endpoints
```

## Map on a client

```csharp
await using var endpoint = client.MapMqtt("sensors/{deviceId:int}/reading", async ctx =>
{
    var deviceId = ctx.Route.GetInt("deviceId");
    await StoreReadingAsync(deviceId, ctx.Message.Payload, ctx.CancellationToken);
});

await endpoint.Subscribed;
```

## Map on a host

```csharp
builder.Services.AddPulseMqttClient("telemetry", options =>
{
    options.Host = "broker.example.com";
    options.ClientId = "telemetry-service";
});

var app = builder.Build();

app.MapMqtt("sensors/{deviceId:int}/reading",
    (int deviceId, Reading reading, IDeviceStore store, CancellationToken ct) =>
        store.SaveAsync(deviceId, reading, ct));
```

Handler parameters are bound by the source generator: route values, typed payloads, services, `MqttEndpointContext`, and `CancellationToken`.

## Manual acknowledgement endpoint

Endpoints are automatic by default. Opt into manual acknowledgement per endpoint when persistence or processing must complete before PUBACK/PUBREC is sent.

```csharp
app.MapMqtt("orders/{id}", async ctx =>
{
    await PersistAsync(ctx.Message, ctx.CancellationToken);
    await ctx.AcknowledgeAsync(ctx.CancellationToken);
}, new MqttEndpointOptions
{
    QualityOfService = MqttQualityOfService.AtLeastOnce,
    Acknowledgement = MqttAcknowledgementMode.Manual,
});
```

In automatic mode, `AcknowledgeAsync` and `RejectAsync` throw `InvalidOperationException`. Request/reply endpoints stay automatic.

Full docs: https://araxis.github.io/pulse-mqtt/packages/endpoints
