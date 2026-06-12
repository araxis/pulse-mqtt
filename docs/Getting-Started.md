# Getting started

## Install

```
dotnet add package Pulse.Mqtt.Client
dotnet add package Pulse.Mqtt.DependencyInjection
dotnet add package Pulse.Mqtt.Serialization.Json
```

`Pulse.Mqtt.Core` comes in transitively. Add `Pulse.Mqtt.Resilience.Polly`,
`Pulse.Mqtt.Transport.WebSocket`, or `Pulse.Mqtt.Testing` when you need them.

## Register a client

```csharp
builder.Services
    .AddPulseMqttClient("devices", options =>
    {
        options.Host = "broker.example.com";
        options.Port = 1883;
        options.ClientId = "my-service";
        options.KeepAliveSeconds = 30;
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default));
```

The client starts and stops with the host (`IHostedService`), connects in the background, and
reconnects on its own. Resolve it anywhere:

```csharp
var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");
```

### Controlling the lifecycle yourself

To start and stop the client explicitly instead — on a feature flag, a UI toggle, a schedule —
opt out of the automatic start:

```csharp
options.StartWithHost = false;
```

then drive it whenever you want:

```csharp
await client.StartAsync(token);   // begins connecting in the background
await client.StopAsync(token);    // disconnects and stops reconnecting
await client.StartAsync(token);   // start again later — restart is fully supported
```

`StartWithHost = true` (the default) and manual calls compose: `StopAsync` is idempotent and a
stopped client can always be restarted. Host shutdown stops a running client in both modes.

## Publish and subscribe

```csharp
// Typed publish, awaited to the broker acknowledgement at QoS 1.
await client.PublishAsync("sensors/boiler-1/telemetry", reading, MqttQualityOfService.AtLeastOnce);

// Routed subscription: {deviceId} is captured from each matching topic.
using var route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    (reading, message, token) =>
    {
        Console.WriteLine($"{message.Values["deviceId"]}: {reading.Value}");
        return ValueTask.CompletedTask;
    });
```

Every publish returns a `PublishOutcome`: `Delivered` (acknowledged at QoS > 0), `Queued`
(offline; flushes after reconnect, after re-subscription), or `DroppedOffline` (QoS 0 while
offline, when configured to drop).

## Connection state

```csharp
await foreach (var change in client.WatchState(token))
{
    logger.LogInformation("{Previous} -> {Current}", change.Previous, change.Current);
}
```

## The sample

[`samples/Pulse.Mqtt.Sample`](../samples/Pulse.Mqtt.Sample) exercises all of the above plus
request/response. It runs with no setup at all (in-process broker), or against a real one:

```
dotnet run --project samples/Pulse.Mqtt.Sample            # in-process broker
dotnet run --project samples/Pulse.Mqtt.Sample -- --host localhost --port 1883
```
