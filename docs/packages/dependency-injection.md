# Dependency injection package

Package: `Pulse.Mqtt.DependencyInjection`

Use this package when an application is hosted and should resolve named MQTT clients from the
service container.

## Install

```shell
dotnet add package Pulse.Mqtt.DependencyInjection
```

## Register a client

```csharp
builder.Services
    .AddPulseMqttClient("devices", options =>
    {
        options.Host = "broker.example.com";
        options.Port = 1883;
        options.ClientId = "device-worker";
    })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default))
    .AddHealthCheck();
```

Resolve the client by name:

```csharp
var client = provider
    .GetRequiredService<IPulseMqttClientFactory>()
    .GetClient("devices");
```

The package also exposes per-client swap points:

```csharp
.UseTransportFactory(_ => transportFactory)
.UseReconnectStrategy(_ => reconnectStrategy)
.UseSessionStore(_ => sessionStore)
.UseMessageStore(_ => messageStore)
.UseSerializer(_ => serializer)
```

See [Dependency injection](/guide/dependency-injection) for options binding, multiple clients,
host-managed lifecycle, and health checks.
