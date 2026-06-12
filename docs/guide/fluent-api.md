# Fluent API

Every operation in Pulse has a direct form (options records, explicit packets) and a fluent
form. The fluent layer is a thin set of builders over the same APIs — same semantics, same
guarantees, no reflection, fully Native AOT safe. Use whichever reads better; mix freely.

## Building a client

For use without dependency injection, `PulseMqttClientBuilder` replaces hand-assembling the
options records:

```csharp
await using var client = await new PulseMqttClientBuilder()
    .WithTcp("broker.example.com", 8883, useTls: true)
    .WithClientId("service-1")
    .WithCredentials("device-42", "secret")
    .WithKeepAlive(TimeSpan.FromSeconds(30))
    .WithCleanStart(false)
    .WithSerializer(new JsonMqttSerializer(AppJsonContext.Default))
    .WithBackoff(TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(30))
    .WithOfflineQueue(capacity: 2048, OverflowPolicy.DropOldest)
    .BuildAndStartAsync(ct);
```

Everything configurable on [`ResilientMqttClientOptions`](/reference/options#resilientmqttclientoptions)
has a method: the swap points (`WithReconnectStrategy`, `WithReconnectDecision`,
`WithLifecycle`, `WithSessionStore`, `WithMessageStore`), `WithTransport` for WebSocket or the
in-process test broker, `WithLogger`, `WithTimeProvider` for fake-clock tests,
`WithRawOptions` for handshake timeouts, and `WithConnect` as the full-CONNECT escape hatch
(wills, session expiry, enhanced auth).

`Build()` returns the client unstarted; `BuildAndStartAsync(ct)` starts it (connection proceeds
in the background, as always). Validation is explicit: no transport or no identity fails with
a message naming the missing call, and mixing `WithConnect` with the individual identity
methods is rejected rather than silently merged.

::: tip With dependency injection
Registered clients already have a fluent surface — `AddPulseMqttClient(...)` returns a builder
with the same swap methods. See [Dependency injection](./dependency-injection).
:::

## Publishing

```csharp
var outcome = await client.Publish("sensors/boiler-1/telemetry")
    .AtLeastOnce()                          // or .ExactlyOnce(), .WithQualityOfService(...)
    .WithRetain()
    .WithMessageExpiry(TimeSpan.FromMinutes(5))
    .WithUserProperty("tenant", "acme")
    .WithPayload(reading)                   // typed: serializes and stamps content type
    .SendAsync(ct);
```

`WithPayload` takes a typed value (through the configured serializer), a `string` (UTF-8, with
the payload format stamped), or raw bytes. `WithContentType`, `WithResponseTopic`, and
`WithCorrelationData` cover the remaining MQTT 5 properties. `SendAsync` returns the same
`PublishOutcome` as `PublishAsync` — `Delivered`, `Queued`, or `DroppedOffline`,
[never silent](./publishing#outcomes--no-silent-loss).

## Routing

```csharp
using var route = await client.Route("sensors/{deviceId}/temp")
    .WithQueue(capacity: 128, RouteOverflow.DropOldest)
    .WithConcurrency(4)
    .WithSubscriptionQualityOfService(MqttQualityOfService.AtLeastOnce)
    .HandleAsync<TelemetryReading>((reading, message, ct) =>
        Handle(reading, message.Values["deviceId"]));
```

Terminals: `HandleAsync` (raw handler), `HandleAsync<T>` (typed), and `StreamAsync` for the
`await foreach` form. Each registers the route and subscribes its filter, exactly like
`OnAsync`/`OpenStreamAsync`; everything from [Routing](./routing) — bounded queues, overflow,
fault isolation — applies unchanged.

## Request and response

```csharp
var reply = await client.Request("devices/boiler-1/status")
    .WithTimeout(TimeSpan.FromSeconds(5))
    .WithQualityOfService(MqttQualityOfService.AtLeastOnce)
    .SendAsync<StatusRequest, StatusReply>(new StatusRequest("dashboard"), ct);
```

The raw terminal pairs with `WithPayload`/`WithContentType` when payloads are untyped:

```csharp
MqttPublishPacket raw = await client.Request("devices/boiler-1/status")
    .WithPayload(requestBytes)
    .SendAsync(ct);
```

Correlation, the private reply subscription, and timeouts behave exactly as in
[Request and response](./request-response).

## Design notes

- Builders are plain mutable classes returning `this` — no expression trees, no reflection,
  nothing for the trimmer to warn about.
- Each terminal delegates to the corresponding client method, so behavior, diagnostics, and
  outcomes are identical between the fluent and direct forms.
- The route and request builders reuse the options records (`MqttRouteOptions`,
  `MqttRequestOptions`); the client builder produces a regular `ResilientMqttClientOptions`.
  There is no second configuration model to learn — the DSL is shorthand, not a dialect.
