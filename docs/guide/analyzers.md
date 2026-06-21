# Analyzers

`Pulse.Mqtt.Analyzers` is an optional C# analyzer package. It does not change runtime behavior and
is not referenced by the client packages; add it only to projects where you want compile-time
guidance.

## Install

```shell
dotnet add package Pulse.Mqtt.Analyzers
```

For project files, keep the analyzer private to the consuming project. If the project does not
use central package management, keep the version that `dotnet add package` inserted:

```xml
<PackageReference Include="Pulse.Mqtt.Analyzers" PrivateAssets="all" />
```

## Diagnostics

| ID | Default | What it catches | Usual fix |
| --- | --- | --- | --- |
| `PMQ0001` | Warning | A Pulse MQTT async operation returning `Task` or `ValueTask` is called as a bare statement. | `await` it, return it, assign it, pass it to an API such as `Task.WhenAll`, or use `_ =` for an intentional fire-and-forget call. |
| `PMQ0002` | Warning | A Pulse MQTT async operation has an optional `CancellationToken`, a caller token is in scope, and the call omits it. | Pass the caller token. |
| `PMQ0003` | Warning | A known async-owned MQTT resource is disposed through synchronous `Dispose()` or regular `using`. | Use `await using` or `DisposeAsync`. |

## Examples

Observe publish and connect operations:

```csharp
// PMQ0001
client.PublishAsync(packet);

// OK
await client.PublishAsync(packet, cancellationToken);
```

Propagate the caller token when one is available:

```csharp
// PMQ0002
public async Task PublishAsync(ResilientMqttClient client, CancellationToken cancellationToken)
{
    await client.PublishAsync(packet);
}

// OK
public async Task PublishAsync(ResilientMqttClient client, CancellationToken cancellationToken)
{
    await client.PublishAsync(packet, cancellationToken);
}
```

Dispose async-owned resources asynchronously:

```csharp
// OK
await using var route = await client.OnAsync("devices/{id}/state", HandleAsync, cancellationToken);

// OK: local route registrations are synchronous handles.
using var registration = client.RegisterRoute("devices/{id}/state", HandleAsync);
```

`PMQ0003` intentionally does not flag synchronous route-registration handles returned from
`RegisterRoute`, `RegisterRequestHandler`, or `RegisterRequestStreamHandler`; those handles are
ordinary `IDisposable` registrations.

## Suppression

Prefer a local suppression when the code is intentionally outside the normal pattern:

```csharp
#pragma warning disable PMQ0001
client.PublishAsync(packet);
#pragma warning restore PMQ0001
```

Or configure severity in `.editorconfig`:

```ini
dotnet_diagnostic.PMQ0001.severity = none
dotnet_diagnostic.PMQ0002.severity = suggestion
dotnet_diagnostic.PMQ0003.severity = warning
```
