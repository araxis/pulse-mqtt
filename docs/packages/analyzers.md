# Analyzer package

Package: `Pulse.Mqtt.Analyzers`

Use this package when projects should get compiler warnings for common Pulse MQTT usage mistakes.
It is opt-in and is not referenced by the runtime packages.

## Install

```shell
dotnet add package Pulse.Mqtt.Analyzers
```

Keep the analyzer private in project files:

```xml
<PackageReference Include="Pulse.Mqtt.Analyzers" PrivateAssets="all" />
```

## Diagnostics

| ID | What it catches |
| --- | --- |
| `PMQ0001` | A Pulse MQTT async operation is called as a bare statement and not awaited, returned, assigned, passed, or explicitly discarded. |
| `PMQ0002` | A cancellable Pulse MQTT async API omits an available in-scope `CancellationToken`. |
| `PMQ0003` | Known async-owned MQTT resources are disposed synchronously. |

See [Analyzers](/guide/analyzers) for fixes, suppression, and examples.
