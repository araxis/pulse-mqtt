# Reconnect policy package

Package: `Pulse.Mqtt.Resilience.Polly`

Use this package when reconnect timing should be driven by an existing resilience pipeline instead
of the built-in backoff strategy.

## Install

```shell
dotnet add package Pulse.Mqtt.Resilience.Polly
```

## Configure

```csharp
.UseReconnectStrategy(_ => new PollyReconnectStrategy(pipeline))
```

The reconnect strategy controls retry counts, delays, jitter, and circuit breaking. The resilient
client still owns state transitions, lifecycle hooks, re-subscription, offline queue flush, and
sticky terminal faults.

Configure the pipeline so final authentication or identity failures escape rather than retrying
forever.

See [Resilience](/guide/resilience#backoff) and [Extending](/guide/extending#custom-reconnect-strategy).
