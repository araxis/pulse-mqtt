# Dependency injection package

Package: `Pulse.Mqtt.DependencyInjection`

Use this package when an application is hosted with `Microsoft.Extensions.*` and should resolve
named MQTT clients from the service container.

## Install

```shell
dotnet add package Pulse.Mqtt.DependencyInjection
```

Most hosted services also add one serializer package:

```shell
dotnet add package Pulse.Mqtt.Serialization.Json
```

## Register a client

```csharp
builder.Services
    .AddPulseMqttClient("devices", options =>
    {
        options.Host = "broker.example.com";
        options.Port = 1883;
        options.ClientId = "device-worker";
        options.KeepAliveSeconds = 30;
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default))
    .AddHealthCheck();
```

The registration creates one singleton `ResilientMqttClient` per name, an
`IPulseMqttClientFactory`, and a hosted-service adapter unless `ConnectWithHost` is disabled.

## Resolve clients

Resolve by factory:

```csharp
var client = provider
    .GetRequiredService<IPulseMqttClientFactory>()
    .GetClient("devices");
```

Or resolve the keyed client directly:

```csharp
public sealed class TelemetryWorker(
    [FromKeyedServices("devices")] ResilientMqttClient client)
{
}
```

## Bind options

`PulseMqttClientOptions` is bindable from configuration:

```json
{
  "Mqtt": {
    "Devices": {
      "Host": "broker.example.com",
      "Port": 8883,
      "UseTls": true,
      "ClientId": "device-worker",
      "KeepAliveSeconds": 30,
      "CleanStart": true,
      "ConnectWithHost": true
    }
  }
}
```

```csharp
builder.Services.AddPulseMqttClient(
    "devices",
    builder.Configuration.GetSection("Mqtt:Devices").Bind);
```

Set `ConnectWithHost = false` when the application wants to call `ConnectAsync` and
`DisconnectAsync` manually.

## Swap per-client behavior

Every swap is keyed by client name, so different named clients can use different stores,
transports, serializers, and policies:

```csharp
builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseTransportFactory(_ => transportFactory)
    .UseReconnectStrategy(_ => reconnectStrategy)
    .UseReconnectDecision(_ => reconnectDecision)
    .UseLifecycle(_ => lifecycle)
    .UseSessionStore(_ => sessionStore)
    .UseMessageStore(_ => messageStore)
    .UseWillProvider<DeviceWillProvider>()
    .UseSerializer(_ => serializer);
```

The factory overloads receive `IServiceProvider`, so custom implementations can resolve their
own dependencies. The generic `UseWillProvider<TProvider>()` overload registers the provider as a
keyed singleton for that client.

## Health checks

State-only health checks preserve the default mapping:

```csharp
.AddHealthCheck()
```

Use threshold options when readiness should degrade or fail on queue pressure:

```csharp
.AddHealthCheck(options =>
{
    options.DegradedOfflineQueueDepthThreshold = 100;
    options.UnhealthyOfflineQueueDepthThreshold = 1_000;
    options.DegradedPendingSubscriptionOperationsThreshold = 10;
});
```

Health-result data includes connection state, attempt, client id, offline queue counters when
available, and broker capability data while connected.

## Operational notes

- The hosted adapter connects clients with the host and disconnects them during shutdown.
- A bad host, port, or client id fails when the named client is first resolved.
- Package references remain opt-in; this package does not pull in durable storage, serializers,
  or alternate transports.
- Named clients are independent singletons. Use separate names for separate brokers or wire
  formats.

## Related docs

- [Dependency injection guide](/guide/dependency-injection)
- [Health checks](/guide/health-checks)
- [Options reference](/reference/options)
