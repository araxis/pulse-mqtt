# Analyzer package

Package: `Pulse.Mqtt.Analyzers`

Use this package when projects should get C# compiler warnings for common Pulse MQTT usage
mistakes. It is opt-in and is not referenced by the runtime packages.

## Install

```shell
dotnet add package Pulse.Mqtt.Analyzers
```

Keep analyzer references private:

```xml
<PackageReference Include="Pulse.Mqtt.Analyzers" PrivateAssets="all" />
```

When using central package management, keep the version in `Directory.Packages.props` and the
private asset marker in the project file.

## Diagnostics

| ID | Default severity | What it catches |
| --- | --- | --- |
| `PMQ0001` | Warning | A Pulse MQTT async operation is called as a bare statement and not awaited, returned, assigned, passed, or explicitly discarded. |
| `PMQ0002` | Warning | A cancellable Pulse MQTT async API omits an available in-scope `CancellationToken`. |
| `PMQ0003` | Warning | Known async-owned MQTT resources are disposed synchronously. |
| `PMQ0004` | Warning | An explicit MQTT 3.1.1 packet initializer sets a known MQTT 5-only packet property. |

## Common fixes

Await async operations:

```csharp
await client.PublishAsync(packet, cancellationToken);
```

Pass the available cancellation token:

```csharp
await client.SubscribeAsync(filters, cancellationToken);
```

Dispose async-owned resources asynchronously:

```csharp
await using var source = client.ToRouteSourceBlock(template, cancellationToken: cancellationToken);
```

Avoid MQTT 5-only properties on MQTT 3.1.1 packets:

```csharp
var publish = new MqttPublishPacket
{
    ProtocolVersion = MqttProtocolVersion.V311,
    Topic = "events/device-7",
};
```

Use protocol feature guards when the code supports both protocol versions.

## Suppression

Prefer fixing the code. Suppress only when the analyzer cannot see a deliberate application
pattern:

```ini
[*.cs]
dotnet_diagnostic.PMQ0002.severity = none
```

Or suppress a narrow region:

```csharp
#pragma warning disable PMQ0002
await client.ConnectAsync();
#pragma warning restore PMQ0002
```

## Operational notes

- The package is C# only.
- Diagnostics are warning-only by default.
- The package has no runtime assets and should not be referenced by shipping libraries as a
  transitive dependency.

## Related docs

- [Analyzer guide](/guide/analyzers)
- [Protocol compatibility](/reference/protocol-compatibility)
- [Package add-ons](/guide/package-add-ons)
