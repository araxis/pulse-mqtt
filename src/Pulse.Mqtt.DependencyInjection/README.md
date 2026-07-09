# Pulse.Mqtt.DependencyInjection

Hosting integration for Pulse MQTT clients. Use it when an application already uses `Microsoft.Extensions.DependencyInjection`, hosted services, options binding, logging, and health checks.

## Install

```shell
dotnet add package Pulse.Mqtt.DependencyInjection
```

## Register a named client

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", options =>
    {
        options.Host = "broker.example.com";
        options.Port = 8883;
        options.UseTls = true;
        options.ClientId = "telemetry-worker";
    })
    .AddHealthCheck();
```

The registration creates a singleton `ResilientMqttClient`, an `IPulseMqttClientFactory`, hosted lifecycle wiring, and optional health checks for that client.

## Resolve a client

```csharp
var client = provider
    .GetRequiredService<IPulseMqttClientFactory>()
    .GetClient("telemetry");
```

Named clients can use different serializers, transports, reconnect strategies, durable stores, and last-will providers.

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default))
    .UseMessageStore(_ => messageStore)
    .UseSessionStore(_ => sessionStore);
```

## Configuration binding

```json
{
  "Mqtt": {
    "Telemetry": {
      "Host": "broker.example.com",
      "Port": 8883,
      "UseTls": true,
      "ClientId": "telemetry-worker"
    }
  }
}
```

```csharp
builder.Services.AddPulseMqttClient(
    "telemetry",
    builder.Configuration.GetSection("Mqtt:Telemetry").Bind);
```

Full docs: https://araxis.github.io/pulse-mqtt/packages/dependency-injection
