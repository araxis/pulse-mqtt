# Endpoints package

Package: `Pulse.Mqtt.Endpoints`

Minimal-API-style endpoints for MQTT: one `MapMqtt` call subscribes a route template's filter
and registers its handler, with typed route constraints and a service scope per message —
the same mental model as `app.MapGet`, aimed at topics instead of URLs.

## Install

```shell
dotnet add package Pulse.Mqtt.Endpoints
```

## Map on the client

The core surface lives on `ResilientMqttClient` and needs no hosting at all:

```csharp
await using var endpoint = client.MapMqtt("sensors/{deviceId:int}/temp", ctx =>
{
    var id = ctx.Route.GetInt("deviceId");     // typed: the constraint guaranteed it parses
    var text = Encoding.UTF8.GetString(ctx.Message.Payload.Span);
    Console.WriteLine($"{id}: {text}");
    return ValueTask.CompletedTask;
});

await endpoint.Subscribed;                     // optional: fail fast on a denied subscription
```

The typed overload deserializes the payload with the client's configured serializer, exactly
like `RegisterRoute<T>`:

```csharp
client.MapMqtt<Reading>("sensors/{deviceId:int}/reading", (reading, ctx) =>
    Store.SaveAsync(ctx.Route.GetInt("deviceId"), reading, ctx.CancellationToken));
```

## Map on the host

`app.MapMqtt(...)` is a thin helper over the client surface: it resolves the registered client
and flows the host's services in, so every invocation gets **its own service scope** — scoped
services behave exactly as they do in an ASP.NET Core request:

```csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddPulseMqttClient("telemetry", o => { o.Host = "broker"; o.ClientId = "svc-1"; });
builder.Services.AddScoped<IDeviceStore, DeviceStore>();

var app = builder.Build();

app.MapMqtt<Reading>("sensors/{deviceId:int}/reading", (reading, ctx) =>
    ctx.Services.GetRequiredService<IDeviceStore>().SaveAsync(ctx.Route.GetInt("deviceId"), reading, ctx.CancellationToken));

await app.RunAsync();
```

With one registered client the name is inferred; with several, name it:
`app.MapMqtt("telemetry", template, handler)`.

## Route constraints

`{name:constraint}` restricts what a parameter level matches — a non-conforming topic never
reaches the handler. The set is closed and every check is a culture-invariant `TryParse`, so
matching stays reflection-free and Native-AOT-clean:

| Constraint | Matches | Typed accessor |
| --- | --- | --- |
| `{id:int}` | invariant `int` | `ctx.Route.GetInt("id")` |
| `{id:long}` | invariant `long` | `ctx.Route.GetLong("id")` |
| `{id:guid}` | `Guid` | `ctx.Route.GetGuid("id")` |
| `{flag:bool}` | `true` / `false` | `ctx.Route.GetBool("flag")` |
| `{name}` | any single level | `ctx.Route.GetString("name")` |

Constraints work everywhere templates do — `RegisterRoute`, `OpenRouteStream`, and request
handlers included.

## What one map call does

- Parses the template and registers the local route (`RegisterRoute` underneath — dispatch,
  bounded queues, and fault isolation are the existing machinery, unchanged).
- Subscribes the matching filter with the options you pass (`QualityOfService` defaults to
  at-least-once). Offline, the subscription is queued and applied on the next connection.
- Returns an `MqttEndpoint`: `Subscribed` completes when the broker granted (or queued) the
  subscription and faults if it was denied; disposing unregisters the route and unsubscribes.

## Minimal-API-style handler signatures

The package ships a source generator, so handlers can also be written the way `app.MapGet`
handlers are — name the parameters you need, in any order:

```csharp
app.MapMqtt("sensors/{deviceId:int}/reading",
    (int deviceId, Reading reading, IDeviceStore store, CancellationToken ct) =>
        store.SaveAsync(deviceId, reading, ct));
```

The generator lowers every such call onto the context API above **at compile time**, using C#
interceptors — there is no runtime binder and no reflection, so the zero-AOT-warning guarantee
holds by construction (the AOT smoke binary maps one of these). A call site the generator
cannot bind is a **compile error** (`PMQE001`–`PMQE006`), never a silent fallback.

How parameters bind:

| Parameter | Binds to |
| --- | --- |
| name matches a `{route}` parameter | the route value, typed by its constraint |
| `CancellationToken` | the dispatch token |
| `MqttEndpointContext` | the context itself |
| `MqttPublishPacket` | the raw message |
| first other complex type | the payload, via the configured serializer |
| further complex types | services from the per-message scope |
| `[FromRoute]`, `[FromPayload]`, `[FromServices]` | explicit override for any of the above |

Handlers may return `void`, `Task`, or `ValueTask`. Route-bound parameters must use the
matching constraint (`int deviceId` needs `{deviceId:int}`), so a non-conforming topic is
rejected before the handler rather than failing inside it. The template must be a constant
string — it is parsed at compile time.

The explicit `MqttEndpointContext` overloads remain the stable foundation: they are what the
generator emits into, and what to use when a handler is built dynamically.

## Related docs

- [Routing](/guide/routing) — the dispatch model underneath
- [Typed messaging](/guide/typed-messaging) — serializers for payload binding
- [Dependency injection](/packages/dependency-injection) — registering named clients
