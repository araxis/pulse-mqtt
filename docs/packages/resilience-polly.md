# Reconnect policy package

Package: `Pulse.Mqtt.Resilience.Polly`

Use this package when reconnect timing should be driven by an existing resilience pipeline instead
of the built-in exponential backoff strategy.

## Install

```shell
dotnet add package Pulse.Mqtt.Resilience.Polly
```

## What it provides

| Type | Contract | Purpose |
| --- | --- | --- |
| `PollyReconnectStrategy` | `IReconnectStrategy` | Runs each connection attempt through a Polly v8 `ResiliencePipeline`. |

The resilient client still owns state transitions, lifecycle hooks, re-subscription, offline
queue flush, publish redelivery, and sticky terminal faults. The package only replaces the retry
timing loop.

## Configure a pipeline

```csharp
using Polly;
using Polly.Retry;
using Pulse.Mqtt.Resilience.Polly;

var pipeline = new ResiliencePipelineBuilder()
    .AddRetry(new RetryStrategyOptions
    {
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        MaxRetryAttempts = int.MaxValue,
        Delay = TimeSpan.FromMilliseconds(500),
        MaxDelay = TimeSpan.FromSeconds(30),
    })
    .Build();
```

Register the strategy:

```csharp
builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline));
```

Direct construction uses the same option:

```csharp
new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "devices" },
    ReconnectStrategy = new PollyReconnectStrategy(pipeline),
};
```

## Retry classification

`PollyReconnectStrategy` controls retry timing. `IReconnectDecision` still classifies whether an
MQTT failure is retryable or terminal. Keep authentication and identity failures terminal unless
the application has a real token-refresh path:

```csharp
.UseReconnectDecision(_ => new TokenAwareReconnectDecision(tokens))
```

If a decision says "do not retry", the client faults instead of feeding the attempt back into the
pipeline.

## Operational notes

- Avoid unbounded rapid retries. Always include a delay, jitter, or circuit behavior.
- Keep retry policies cancellation-aware; host shutdown should stop promptly.
- Do not hide terminal broker rejections behind forever-retry policies.
- Use diagnostics snapshots and health checks to observe reconnect pressure.

## Related docs

- [Resilience](/guide/resilience#backoff)
- [Extending reconnect strategy](/guide/extending#reconnect-strategy)
- [Health checks](/guide/health-checks)
