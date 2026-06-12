# Configuration and swapping

Every major behavior sits behind a small interface with a solid default. Swapping one is a
single builder call; nothing else changes.

| Behavior | Contract | Default | Builder call |
| --- | --- | --- | --- |
| Transport | `IMqttTransportFactory` | TCP with optional TLS | `UseTransportFactory` |
| Reconnect policy | `IReconnectStrategy` | Exponential backoff, full jitter | `UseReconnectStrategy` |
| Retry classification | `IReconnectDecision` | Retry network faults, stop on auth failures | `UseReconnectDecision` |
| Connection-up/down hooks | `IConnectionLifecycle` | Re-subscribe before queued publishes flush | `UseLifecycle` |
| Durable subscriptions | `ISessionStore` | In-memory | `UseSessionStore` |
| Offline queue | `IMessageStore` | Bounded in-memory queue | `UseMessageStore` |
| Payload serialization | `IMqttSerializer` | none (bring one) | `UseSerializer` |

## Swap the reconnect policy for a Polly pipeline

```csharp
builder.Services
    .AddPulseMqttClient("devices", options => { /* ... */ })
    .UseReconnectStrategy(_ => new PollyReconnectStrategy(
        new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                MaxRetryAttempts = int.MaxValue,
            })
            .Build()));
```

## Swap the transport for WebSocket

```csharp
.UseTransportFactory(_ => new WebSocketTransportFactory(new WebSocketTransportOptions
{
    Uri = new Uri("wss://broker.example.com/mqtt"),
}))
```

## Offline queue behavior

`ResilientMqttClientOptions.OfflineQueue` bounds the queue and picks the overflow policy:
`Wait` (backpressure), `DropOldest`, or `DropNewest`. QoS 0 messages drop by default while
offline; set `IncludeQos0` to queue them too.

## Options binding

`PulseMqttClientOptions` is named-options friendly:

```csharp
builder.Services
    .AddPulseMqttClient("devices", builder.Configuration.GetSection("Mqtt:Devices").Bind);
```

## Health checks

```csharp
.AddHealthCheck()
```

registers a health check named after the client that reports the connection state.
