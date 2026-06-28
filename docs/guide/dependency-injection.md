# Dependency injection

Package `Pulse.Mqtt.DependencyInjection` integrates with `Microsoft.Extensions.*`: named
clients, options binding, hosted lifecycle, health checks, and per-client behavior swaps —
the same shape as `IHttpClientFactory`.

## Registration

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

Each name registers one singleton `ResilientMqttClient` (created lazily, validated on first
resolve with clear messages), an optional hosted lifecycle adapter, and a keyed-service entry.

## Resolving clients

```csharp
// The factory — mirrors IHttpClientFactory:
var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");

// Or as a keyed service:
public sealed class TelemetryService([FromKeyedServices("devices")] ResilientMqttClient client) { }
```

## Options

`PulseMqttClientOptions` binds from configuration like any named options:

```csharp
builder.Services.AddPulseMqttClient("devices",
    builder.Configuration.GetSection("Mqtt:Devices").Bind);
```

```json
{
  "Mqtt": {
    "Devices": {
      "Host": "broker.example.com",
      "Port": 8883,
      "UseTls": true,
      "ClientId": "my-service",
      "Username": "device-42",
      "Password": "secret",
      "KeepAliveSeconds": 30,
      "CleanStart": true,
      "ConnectWithHost": true
    }
  }
}
```

The full set: `Host`, `Port`, `UseTls`, `ClientId`, `KeepAliveSeconds`, `CleanStart`,
`Username`, `Password`, `ProtocolVersion`, `ConnectWithHost`. Swappable behaviors are configured
on the builder, not in options — code, not strings.

## Swapping behaviors per client

Every swap point has a builder method; each receives the `IServiceProvider`:

```csharp
builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseTransportFactory(sp => new WebSocketTransportFactory(wsOptions))
    .UseReconnectStrategy(sp => new PollyReconnectStrategy(pipeline))
    .UseReconnectDecision(sp => new TokenAwareDecision(sp.GetRequiredService<ITokenSource>()))
    .UseLifecycle(sp => new WarmCacheLifecycle(sp.GetRequiredService<ICache>()))
    .UseSessionStore(sp => new SqliteSessionStore(connectionString))
    .UseMessageStore(sp => new SqliteMessageStore(connectionString))
    .UseWillProvider<DeviceWillProvider>()
    .UseSerializer(sp => new JsonMqttSerializer(AppJsonContext.Default));
```

Swaps are **per client name** — two clients can use different transports, stores, and
policies. Registrations are keyed services under the hood, so they resolve with full DI
support.

## Multiple clients

```csharp
builder.Services.AddPulseMqttClient("telemetry", ConfigureTelemetry);
builder.Services.AddPulseMqttClient("commands", ConfigureCommands)
    .UseReconnectStrategy(_ => new PollyReconnectStrategy(aggressivePipeline));
```

Independent connections, independent settings, independent lifecycles.

The ASP.NET Core sample shows the same named client resolved from Minimal API handlers with
`[FromKeyedServices("telemetry")]`, plus health checks and diagnostics endpoints:
[`samples/Pulse.Mqtt.AspNetCoreSample`](https://github.com/araxis/pulse-mqtt/tree/main/samples/Pulse.Mqtt.AspNetCoreSample).

## Lifecycle

The hosted service connects each client with the host and disconnects it on shutdown. Set
`ConnectWithHost = false` to [drive it manually](./lifecycle#host-managed-or-manual) — host
shutdown still disconnects a running client.

## Logging

When an `ILoggerFactory` is registered, each client logs under
`Pulse.Mqtt.Client.<name>` — state changes, connect failures, connection loss, route handler
faults. All messages are source-generated (zero cost when the level is off).

## Health checks

```csharp
builder.Services.AddPulseMqttClient("devices", configure)
    .AddHealthCheck();
```

Registers a check named `pulse-mqtt-<name>` that maps the connection state to `Healthy` /
`Degraded` / `Unhealthy`. Use the overload when readiness should also react to offline backlog,
dropped queued publishes, or pending subscription operations:

```csharp
builder.Services.AddPulseMqttClient("devices", configure)
    .AddHealthCheck(options =>
    {
        options.DegradedOfflineQueueDepthThreshold = 100;
        options.UnhealthyOfflineQueueDepthThreshold = 1_000;
    });
```

The full mapping, threshold precedence, nullable queue-counter behavior, and liveness-vs-readiness
split are in [Health checks](./health-checks).
