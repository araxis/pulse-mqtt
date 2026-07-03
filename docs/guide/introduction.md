# Introduction

Pulse.Mqtt is an MQTT 5.0 / 3.1.1 client for modern .NET (`net8.0` and `net10.0`), built around
three commitments: **resilience is the default**, **every behavior is swappable**, and
**performance is measured, not claimed**.

## Why another MQTT client?

Most .NET MQTT clients hand the application the hard parts:

- Reconnecting after a drop, with sensible backoff, **and** re-subscribing before queued
  messages flush — in the right order, every time.
- Deciding what happens to publishes while offline: queue them, bound the queue, pick an
  overflow policy, and never lose a message silently.
- Routing messages from one inbound stream to the right handler, without one slow consumer
  stalling the rest.
- Typed payloads, request/response, health checks, metrics.

Pulse makes all of that first-class. The result reads like this:

```csharp
services.AddPulseMqttClient("devices", o => { o.Host = "broker"; o.ClientId = "svc-1"; })
    .UseSerializer(_ => new JsonMqttSerializer(AppJsonContext.Default));
```

and from then on the client connects in the background, survives broker restarts,
re-subscribes, flushes its offline queue, routes messages to your handlers, and reports its
health — with nothing else written.

## The swap principle

Every major behavior lives behind a small interface with a solid default:

| Behavior | Contract |
| --- | --- |
| Transport | `IMqttTransportFactory` |
| Reconnect loop | `IReconnectStrategy` |
| Retry vs. fault classification | `IReconnectDecision` |
| Connection up/down hooks | `IConnectionLifecycle` |
| Durable subscriptions | `ISessionStore` |
| Offline queue | `IMessageStore` |
| Payload serialization | `IMqttSerializer` |

Want the reconnect loop to be a Polly pipeline? One line. A durable offline store? Implement
one interface. WebSocket instead of TCP? Swap the factory. Nothing else changes — the
[extending guide](./extending) shows each one.

## Design rules

These hold everywhere in the codebase:

- **Bounded everything.** Inbound queues, per-route queues, the offline queue — all bounded,
  with explicit overflow policies. Backpressure flows to the socket; memory never grows
  without limit.
- **No silent loss.** A publish always tells you what happened: `Delivered`, `Queued`, or
  `DroppedOffline`.
- **Failures are observable.** Sticky faults on terminal errors, state transitions on a
  watchable stream, structured logs, traces, and metrics.
- **Time is injected.** Every timeout and delay goes through `TimeProvider`, so tests run with
  a fake clock and never sleep.
- **Allocation is a budget.** Publish encoding allocates nothing; the wire path is spans and
  pipelines end to end. The [benchmarks](/Benchmark-vs-MQTTnet) are published in full —
  including the scenarios where Pulse does not come out ahead.

## Where to go next

- [Getting started](./getting-started) — install, connect, publish, subscribe in five minutes.
- [Package add-ons](./package-add-ons) — choose storage, pipeline, serializer, transport, testing,
  and analyzer packages.
- [Resilience](./resilience) — what happens when the network misbehaves.
- [Routing](./routing) and [typed messaging](./typed-messaging) — from topics to handlers to objects.
- [Performance](./performance) — the numbers and how they were measured.
