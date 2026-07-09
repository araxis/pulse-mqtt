# Pulse.Mqtt.Analyzers

Optional C# compiler diagnostics for common Pulse MQTT usage mistakes. This package has no runtime assets and should normally be referenced privately.

## Install

```shell
dotnet add package Pulse.Mqtt.Analyzers
```

Keep analyzer references private:

```xml
<PackageReference Include="Pulse.Mqtt.Analyzers" PrivateAssets="all" />
```

## Diagnostics

| ID | Default severity | What it catches |
| --- | --- | --- |
| `PMQ0001` | Warning | A Pulse MQTT async operation is called and not awaited, returned, assigned, passed, or explicitly discarded. |
| `PMQ0002` | Warning | A cancellable Pulse MQTT async API omits an available in-scope `CancellationToken`. |
| `PMQ0003` | Warning | Known async-owned MQTT resources are disposed synchronously. |
| `PMQ0004` | Warning | An explicit MQTT 3.1.1 packet initializer sets a known MQTT 5-only packet property. |

## Example fixes

```csharp
await client.PublishAsync(packet, cancellationToken);
```

```csharp
await using var route = await client.Route("orders/{id}")
    .AtLeastOnce()
    .StreamAsync(cancellationToken);
```

Suppress narrowly only when the analyzer cannot see a deliberate application pattern.

Full docs: https://araxis.github.io/pulse-mqtt/guide/analyzers
