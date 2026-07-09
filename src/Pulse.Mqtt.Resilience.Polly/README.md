# Pulse.Mqtt.Resilience.Polly

Reconnect strategy integration for applications that want Pulse MQTT reconnect timing to be driven by a Polly v8 `ResiliencePipeline`.

## Install

```shell
dotnet add package Pulse.Mqtt.Resilience.Polly
```

## Build a reconnect pipeline

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

## Register it

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline));
```

The resilient client still owns connection state, re-subscription, offline queue flush, publish redelivery, and terminal fault handling. This package only replaces reconnect timing.

## Operational notes

- Include delay and jitter so retry loops do not hammer the broker.
- Keep cancellation flowing through the pipeline so host shutdown is prompt.
- Use `IReconnectDecision` for retry-vs-fault classification; do not hide terminal authentication or identity failures behind a retry policy.

Full docs: https://araxis.github.io/pulse-mqtt/packages/resilience-polly
