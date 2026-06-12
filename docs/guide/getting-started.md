# Getting started

## Install

```shell
dotnet add package Pulse.Mqtt.Client
dotnet add package Pulse.Mqtt.DependencyInjection
dotnet add package Pulse.Mqtt.Serialization.Json
```

`Pulse.Mqtt.Core` comes in transitively. See [Packages](/reference/packages) for the add-ons:
Polly integration, WebSocket transport, and the in-process test broker.

## Register a client

```csharp
var builder = Host.CreateApplicationBuilder(args);

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

The client starts with the host, connects in the background, reconnects on drops, and
re-subscribes on its own. Resolve it anywhere:

```csharp
var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");
```

::: tip Manual control
Prefer to start and stop the client yourself? Set `options.StartWithHost = false` and call
`StartAsync`/`StopAsync` whenever you want — see [Lifecycle and state](./lifecycle).
:::

No host? Construct directly:

```csharp
var factory = new TcpTransportFactory(new TcpTransportOptions { Host = "broker.example.com" });
await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "my-service" },
    Serializer = new JsonMqttSerializer(AppJsonContext.Default),
});
await client.StartAsync(ct);
```

## Publish

```csharp
// Typed, awaited to the broker acknowledgement at QoS 1.
var outcome = await client.PublishAsync(
    "sensors/boiler-1/telemetry",
    new TelemetryReading("C", 21.5, DateTimeOffset.UtcNow),
    MqttQualityOfService.AtLeastOnce);

// outcome.Disposition: Delivered, Queued (offline), or DroppedOffline — never silent.
```

## Subscribe

```csharp
using var route = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    (reading, message, token) =>
    {
        Console.WriteLine($"{message.Values["deviceId"]}: {reading.Value}{reading.Unit}");
        return ValueTask.CompletedTask;
    });
```

The template subscribes `sensors/+/telemetry` and captures `{deviceId}` from every matching
topic. Each route has its own bounded queue, and a throwing handler faults only its route.

## Request and response

```csharp
// One side asks…
var reply = await client.RequestAsync<StatusRequest, StatusReply>(
    "devices/boiler-1/status", new StatusRequest("dashboard"));

// …the other answers.
using var responder = await client.OnRequestAsync<StatusRequest, StatusReply>(
    "devices/{deviceId}/status",
    (request, message, token) =>
        ValueTask.FromResult(new StatusReply(message.Values["deviceId"], "online")));
```

## Watch the connection

```csharp
await foreach (var change in client.WatchState(token))
{
    logger.LogInformation("MQTT: {Previous} -> {Current}", change.Previous, change.Current);
}
```

## Run the sample

[`samples/Pulse.Mqtt.Sample`](https://github.com/araxis/pulse-mqtt/tree/main/samples/Pulse.Mqtt.Sample)
exercises everything above. It needs no infrastructure — with no arguments it runs against the
in-process test broker:

```shell
dotnet run --project samples/Pulse.Mqtt.Sample
dotnet run --project samples/Pulse.Mqtt.Sample -- --host localhost --port 1883
```

## Next steps

- [Connecting](./connecting) — TLS, WebSocket, credentials, protocol versions.
- [Publishing](./publishing) — QoS levels, outcomes, retained messages, MQTT 5 properties.
- [Resilience](./resilience) — reconnect policy, offline queue, sticky faults.
- [Testing](./testing) — millisecond tests with the in-process broker.
