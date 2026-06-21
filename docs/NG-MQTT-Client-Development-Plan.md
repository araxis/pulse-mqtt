# Pulse — Next-Generation .NET MQTT Client
## Full Development Plan

> Name: **Pulse** — packages published under `Pulse.Mqtt.*`. Client-only at v1 (an in-memory test broker is in scope; a production broker is **not**).
> Audience: the engineering team building the library. This document is the source of truth for *why*, *what*, *how*, and *in what order*.

---

## How to read this document

- **Part A — Why & Principles** establishes the motivation and the non-negotiable design rules.
- **Part B — Architecture** defines the layers, the swappable abstractions, and the technology choices (Use / Avoid).
- **Part C — Feature List** is the complete, prioritized catalog of what ships.
- **Part D — The Swappability Contract** is the mechanism that makes *every major part interchangeable* (the core requirement). Worked examples: auto-connect → Polly, logging swap, serialization swap.
- **Part E — Development Phases** is the step-by-step plan. Every **Phase → Step → Task** has an explicit *Definition of Done (DoD)*.
- **Part F — Quality Gates** covers testing, performance, packaging, and release.

**Definition of Done is layered** (defined once, applied everywhere):

| Level | "Done" means |
|---|---|
| **Task DoD** | The smallest unit. Code compiles, the one behavior it adds is covered by a passing test, no new analyzer/trim warnings, XML docs on public API. |
| **Step DoD** | All tasks in the step are Done, the step's public surface is usable end-to-end in a sample/test, and the step's acceptance test passes. |
| **Phase DoD** | All steps Done, `dotnet build -c Release` clean, full test suite green, benchmarks (if any) recorded, the phase's "Implements" statement is demonstrably true via a runnable sample. |

---

# PART A — Why & Principles

## A.0 Research validation & corrections (verified 2026-06-08)

A `deep-research` pass (24 primary sources, adversarial verification) confirmed four facts that **sharpen** this plan. Full cited report: [Competitive-Research-MQTT-Clients.md](Competitive-Research-MQTT-Clients.md).

**Confirmed (high confidence):**
1. **MQTTnet v5 removed the `ManagedMqttClient`** (auto-reconnect, queueing, re-subscription). Maintainers now say *"use the regular client and do the reconnect stuff … via your own code,"* and a v5 managed client is *"not yet planned."* → **Resilience is the flagship opening.** Phase 4 is promoted to the headline differentiator — deep design in [Phase-04-Resilience-Detailed-Design.md](Phase-04-Resilience-Detailed-Design.md).
2. **MQTTnet v5.1.0 (`5.1.0.1559`, 2026-02-04) marked all projects AOT-compatible.** → **AOT is now table-stakes, not a differentiator.** We must ship *proven zero-warning* AOT (MQTTnet only *marks* compatibility — not a guarantee on every path). Demoted from headline differentiator to a hard quality bar (Part F).
3. **MQTTnet v5 split `MqttFactory` → `MqttClientFactory`/`MqttServerFactory`** — the entry point is in flux. → **DI-first construction is a validated, easy differentiator.** Keeps Phase 7 as planned.
4. **MQTTnet v5 dropped all pre-.NET 8 targets** (no netstandard / .NET Framework / 6 / 7) to enable Span/Memory. → We **drop the netstandard idea** and target `net8.0` + `net10.0` to match.

**Not validated — treat as design *leads*, not facts:** every cross-language pattern (rumqtt bounded-channel backpressure, Go `autopaho` `OnConnectionUp/Down` lifecycle callbacks, HiveMQ reactive backpressure), the .NET competitor field (HiveMQtt, M2Mqtt/Paho status, Azure/AWS IoT SDKs), the third-party `MQTTnet.AspNetCore.Routing` (attribute routing), and the entire **MQTT-over-QUIC** landscape were *sourced but did not pass adversarial verification*. They inform design as leads; **QUIC stays P1/opt-in** until a fresh verification pass (see the research report's open questions).

---

## A.1 Why we need a next-generation MQTT client

MQTTnet is the de-facto .NET MQTT library: mature, protocol-complete (3.1.0 / 3.1.1 / 5.0), MIT-licensed, with client + embedded broker. Its maturity is also its ceiling — its API and threading model were shaped by .NET Framework-era idioms (event handlers, custom logging abstractions, builders) and predate the patterns that define modern high-performance .NET. The gaps below are the concrete justification for a new library; each maps to a feature we will ship.

### A.1.1 Concrete weaknesses of MQTTnet (the problem statement)

**API ergonomics & message-handling model**
- **Single global message handler.** All inbound messages funnel through one `ApplicationMessageReceivedAsync` event. There is **no built-in topic-to-handler routing** — every consumer hand-rolls a `switch`/wildcard matcher for `+`/`#`. This is the single biggest day-to-day pain point.
- **Event-based, not stream-based.** No `IAsyncEnumerable<…>` and no `Channel`-based consumption; you cannot `await foreach` a subscription or compose with LINQ/Rx.
- **Builder verbosity & stringly-typed payloads.** Long builder chains; payload is `byte[]`/`ReadOnlySequence<byte>` with no typed message contracts, content-type handling, or (de)serialization pipeline.

**Threading & backpressure**
- The received-message handler is **awaited inline on the receive loop**. A slow handler stalls the connection. No first-class backpressure/concurrency policy (per-topic ordering, bounded parallelism, bounded queues).

**Connection resilience: removed, not just bolted on (v5)** *(verified — §A.0)*
- In v4, resilience lived in a separate `ManagedMqttClient` (limited backoff control, resubscription edge cases, ordering surprises, simplistic `IManagedMqttClientStorage`). In **v5 it was removed entirely** — maintainers recommend *"use the regular client and do the reconnect stuff … via your own code,"* and a managed client is *"not yet planned."* The market leader now ships **no first-class reconnect, offline queue, or auto-resubscribe at all.** This is our single clearest opening (see Phase 4 and its deep-dive).

**Observability is non-idiomatic**
- Logging via a **custom `IMqttNetLogger`** rather than `Microsoft.Extensions.Logging.ILogger`. **No `ActivitySource` tracing, no `Meter`/metrics** (connection state, inflight, queue depth, throughput, reconnect counts).

**No idiomatic DI / hosting story**
- No official `services.AddMqttClient(...)`, no named clients à la `IHttpClientFactory`, no `IOptions<>` binding, no `IHostedService` lifecycle, no `IHealthCheck`.

**Performance & memory**
- Codec is not fully `System.IO.Pipelines`/`Span`-native end-to-end; allocates on hot paths; no zero-copy publish path. **Native AOT / trimming** support is incomplete — a problem for MAUI, Blazor WASM, and edge/serverless.

**MQTT 5 niceties present but under-served**
- Protocol bits exist, but higher-level helpers don't: no **request/response (RPC)** abstraction (despite response-topic + correlation-data in the protocol), manual **topic-alias** management, **flow control (Receive Maximum)** not surfaced as a policy, **subscription identifiers** unused (they are perfect to power routing), only a thin **enhanced-auth** hook (no pluggable SASL/SCRAM or token refresh), **shared-subscription** helpers absent.

**Transports & reach gaps**
- **No MQTT-over-QUIC**, no polished Blazor-WASM-first WebSocket story, no MQTT-SN.

**Testing, docs, stability**
- **No lightweight in-memory loopback** to unit-test the client without spinning the full broker. Thin docs. Meaningful breaking changes across v3→v4→v5 burned upgraders.

### A.1.2 What "next generation" means here (the differentiators)

We do **not** win by matching MQTTnet feature-for-feature. We win on the **layer above the protocol** and on being **idiomatic, fast, observable, resilient, and swappable**:

1. **Topic routing & typed handlers** — the killer feature MQTTnet lacks.
2. **One always-resilient client** — reconnect/resubscribe/offline-queue are the default, not a second client type.
3. **DI + observability out of the box** — `ILogger`, `ActivitySource`, `Meter`, `IOptions`, named clients, `IHealthCheck`.
4. **Fast, proven-AOT-clean codec** — `System.IO.Pipelines`/`Span` throughput with **zero** trim/AOT warnings on every path. *(Parity, not a stand-alone pitch: MQTTnet v5.1 already marks AOT compatibility — §A.0. Our edge is the combination of speed + resilience + routing + DX, hit cleanly.)*
5. **Everything swappable** — see Part D. The default for any capability is replaceable by the user (e.g. auto-connect → a Polly pipeline) without forking.

## A.2 Goals & non-goals

**Goals**
- Full MQTT 3.1.1 + 5.0 client. (3.1.0 best-effort.)
- High throughput, low allocation, Native AOT + trimming clean.
- Idiomatic modern .NET (DI, options, logging, hosting, health, OTel).
- Resilience, routing, typed messaging, and RPC as first-class, **all swappable**.
- Transports: TCP/TLS, WebSocket, QUIC, plus a documented Blazor-WASM path.
- Fast in-memory test harness.

**Non-goals (v1)**
- A production broker (only an in-memory loopback test server).
- MQTT-SN.
- Backward source-compat with MQTTnet (we provide a *migration guide*, not a shim, unless demand proves otherwise).
- A UI. (This is a library.)

## A.3 Guiding principles (non-negotiable)

1. **Swappable by default.** Every major capability is one small interface with a default registered via `TryAdd*`, so a user registration always wins. (Part D.)
2. **DI-first, but usable without DI.** A standalone builder constructs the same object graph for console/library use.
3. **Async-first, cancellation everywhere.** Every public async method takes a `CancellationToken`. Bounded queues, no unbounded buffering.
4. **Boring, explicit, fast.** No reflection magic, no service locator, no hidden static state. Source generators over runtime reflection.
5. **Backpressure is a policy, never a stall.** Slow consumers never block the protocol receive loop.
6. **Observable failures.** Nothing fails silently; everything has a log scope, a trace span, and a metric.
7. **Stable core contract.** `Pulse.Mqtt.Core` abstractions are frozen at v1.0 and follow semver strictly. API churn is the enemy that hurt MQTTnet.

---

# PART B — Architecture

## B.1 Layering

```
┌──────────────────────────────────────────────────────────────┐
│ Pulse.Mqtt.DependencyInjection   AddMqttClient(name), options,     │
│                              named clients, IHostedService     │
├──────────────────────────────────────────────────────────────┤
│ Pulse.Mqtt.Client (high level)   Routing, typed messaging, RPC,    │
│                              resilience, offline queue,         │
│                              backpressure, consumption streams  │
├──────────────────────────────────────────────────────────────┤
│ Pulse.Mqtt.Core (low level)      Raw client, session state, QoS    │
│   - Codec (Span/Pipelines)   state machines, keepalive,        │
│   - Packet model             handshake; ALL ABSTRACTIONS live   │
│   - Abstractions (contracts) here and are frozen at v1          │
├──────────────────────────────────────────────────────────────┤
│ Pulse.Mqtt.Transport.*           ITransport implementations:        │
│   Tcp (in Core) / WebSocket / Quic                              │
└──────────────────────────────────────────────────────────────┘
   Cross-cutting add-ons (opt-in packages):
   Pulse.Mqtt.Diagnostics (OTel)  Pulse.Mqtt.Serialization.{Json,MessagePack,Protobuf}
   Pulse.Mqtt.Persistence.{Sqlite,LiteDb}  Pulse.Mqtt.Resilience.Polly  Pulse.Mqtt.Testing
```

**Dependency rule:** arrows point inward. `Core` depends on nothing but the BCL. `Client` depends on `Core`. Add-ons depend on `Core`/`Client` only. No add-on is referenced by `Core`/`Client`.

## B.2 Package / module map (one reason to change each)

| Package | Owns | Depends on |
|---|---|---|
| `Pulse.Mqtt.Core` | Packet model, Span/Pipelines codec, raw connection engine, **all interchange contracts**, default TCP/TLS transport, in-memory defaults | BCL only |
| `Pulse.Mqtt.Client` | Reconnect engine, resubscribe, offline queue, topic router, typed messaging, consumption streams (Channels/`IAsyncEnumerable`), RPC, backpressure | Core |
| `Pulse.Mqtt.DependencyInjection` | `AddMqttClient`, keyed/named clients, `IOptions` binding, hosted lifecycle, health checks | Client |
| `Pulse.Mqtt.Diagnostics` | `ActivitySource` + `Meter` instrumentation, OTel registration helpers | Core, Client |
| `Pulse.Mqtt.Serialization.Json` | `System.Text.Json` (source-gen) serializer | Core |
| `Pulse.Mqtt.Serialization.MessagePack` / `.Protobuf` | Alternative serializers | Core |
| `Pulse.Mqtt.Persistence.Sqlite` / `.LiteDb` | Durable offline queue + session store | Core |
| `Pulse.Mqtt.Resilience.Polly` | `IReconnectStrategy` / `IRetryPolicy` backed by Polly `ResiliencePipeline` | Core |
| `Pulse.Mqtt.Transport.WebSocket` / `.Quic` | Extra transports | Core |
| `Pulse.Mqtt.Testing` | In-memory loopback transport + lightweight test server + assertions | Core, Client |

## B.3 The interchange contracts (the heart of the design)

Every row is a single small interface in `Pulse.Mqtt.Core.Abstractions`, with a **default** impl and a **how-to-swap** path. This table *is* the swappability promise.

| Capability | Contract | Default impl (registered via `TryAdd`) | Swap to (examples) |
|---|---|---|---|
| Network transport | `IMqttTransport` / `IMqttTransportFactory` | `TcpTransport` (Pipelines + `SslStream`) | `WebSocketTransport`, `QuicTransport`, in-memory loopback |
| Wire codec | `IMqttPacketCodec` | `SpanPacketCodec` (zero-alloc) | custom codec, instrumentation-wrapped codec |
| Reconnect / auto-connect | `IReconnectStrategy` | `BackoffReconnectStrategy` (exp + jitter) | **`PollyReconnectStrategy`**, `NoReconnectStrategy`, fixed-interval |
| Operation retry (publish/subscribe) | `IRetryPolicy` | `DefaultRetryPolicy` | Polly-backed, none |
| Offline / inflight queue | `IMessageStore` | `InMemoryMessageStore` (bounded) | `SqliteMessageStore`, `LiteDbMessageStore` |
| Session state (QoS 2 ids, subs) | `ISessionStore` | `InMemorySessionStore` | `SqliteSessionStore` |
| Payload (de)serialization | `IMqttSerializer` / `IMqttSerializer<T>` | `RawSerializer` (bytes passthrough) | `JsonSerializer`, `MessagePackSerializer`, `ProtobufSerializer` |
| Topic routing | `ITopicRouter` | `TrieTopicRouter` (uses sub-ids) | custom matcher |
| Backpressure / dispatch | `IDeliveryPipeline` | `ChannelDeliveryPipeline` (bounded) | Dataflow-based, per-topic-ordered |
| Logging | `ILogger` (BCL) + `IMqttDiagnostics` | MEL adapter | any `ILoggerProvider`; null logger |
| Tracing/metrics | `IMqttInstrumentation` | no-op | OTel (`Pulse.Mqtt.Diagnostics`) |
| Clock (testability) | `TimeProvider` (BCL) | `TimeProvider.System` | `FakeTimeProvider` in tests |
| Enhanced auth (MQTT5) | `IMqttEnhancedAuthenticator` | none (disabled) | SCRAM/OAuth token provider |
| Client identity / credentials | `IMqttCredentialsProvider` | static from options | rotating token provider |

**Rule:** if a capability is worth a default, it is worth an interface. Nothing in the hot path is `sealed` against substitution at the DI boundary.

## B.4 Technology choices — **Use** vs **Avoid**

### Use
- **Target frameworks:** multi-target **`net8.0` + `net10.0`** (matching MQTTnet v5, which dropped all pre-.NET 8 targets to enable Span/Memory — §A.0). No netstandard / .NET Framework shims; legacy users stay on MQTTnet 4.
- **`System.IO.Pipelines`** for all transport I/O (`PipeReader`/`PipeWriter`).
- **`System.Buffers`**: `ReadOnlySequence<byte>`, `IBufferWriter<byte>`, `ArrayPool<T>`, `MemoryPool<T>` for zero/low-alloc codec.
- **`System.Threading.Channels`** for inbound/outbound queues; **`System.Threading.Tasks.Dataflow`** only where a real multi-stage bounded pipeline with fan-out is needed (delivery dispatch option).
- **`Microsoft.Extensions.*`**: `Logging.Abstractions`, `Options`, `DependencyInjection.Abstractions`, `Hosting.Abstractions`, `Diagnostics.HealthChecks`.
- **`System.Diagnostics.Metrics.Meter`** + **`ActivitySource`** with OTel `messaging.*` semantic conventions.
- **`TimeProvider`** (BCL) for all time/delay — never `Task.Delay` against wall clock directly in testable code.
- **`System.Text.Json` source generators** for the default JSON serializer; **MessagePack-CSharp** / **protobuf-net** behind their own packages.
- **`Polly` v8 `ResiliencePipeline`** — but only inside the optional `Pulse.Mqtt.Resilience.Polly` package, never a Core dependency.
- **`System.Net.Quic`** for QUIC; **`System.Net.WebSockets`** (+ `ClientWebSocket`) for WebSocket.
- **xUnit + Shouldly**; **`Microsoft.Extensions.TimeProvider.Testing`** (`FakeTimeProvider`); **BenchmarkDotNet**; **Testcontainers** only for integration tests against a real broker (e.g. Mosquitto/EMQX image).
- **Nullable enabled, analyzers as errors, `IsAotCompatible=true`, `IsTrimmable=true`** across shippable packages.

### Avoid
- **Reflection on hot paths**, runtime expression compilation, `Activator.CreateInstance` scanning. (Breaks AOT, costs allocations.)
- **MediatR, AutoMapper, FluentValidation, Scrutor** — not needed; explicit handlers/registration instead.
- **A second "managed" client type.** One client, resilient by default (avoid MQTTnet's split).
- **A custom logging abstraction** as the primary surface. Use `ILogger`; only keep a thin internal `IMqttDiagnostics` for high-frequency, allocation-sensitive events.
- **Unbounded queues / fire-and-forget tasks.** Everything bounded, owned, cancellable.
- **`async void`** except documented event-style sinks.
- **Hard dependency on any serializer, persistence, or resilience library in Core/Client.**
- **A production broker** in v1 scope.
- **Public mutable static state / service locator.**
- **Throwing as control flow** on expected paths (use `Result<T>` for expected domain failures at the high-level API; exceptions for truly exceptional).

---

# PART C — Full feature list

Priority: **P0** = v1 must-have, **P1** = v1 stretch / fast-follow, **P2** = post-v1. "Swap point" names the contract from §B.3.

### C.1 Protocol core
- P0 MQTT 3.1.1 + 5.0 client; QoS 0/1/2 publish & subscribe. *(codec)*
- P0 CONNECT/CONNACK with full MQTT5 properties; keepalive + PINGREQ/PINGRESP.
- P0 Will message, retained messages, clean-start / session-present handling.
- P0 Session expiry, message expiry, maximum-packet-size, topic-alias-maximum negotiation.
- P0 SUBSCRIBE/UNSUBSCRIBE with all MQTT5 sub options (NL, RAP, retain-handling).
- P1 MQTT 3.1.0 best-effort.

### C.2 Connection & resilience
- P0 **One always-resilient client.** Auto-connect on start. *(`IReconnectStrategy`)*
- P0 Exponential backoff + jitter default; **swap to Polly pipeline**.
- P0 Automatic resubscribe after reconnect; session-state restore. *(`ISessionStore`)*
- P0 Observable `ConnectionState` (stream + current value).
- P0 Durable offline outbound queue, bounded, pluggable store. *(`IMessageStore`)*
- P1 Flow control: honor server Receive-Maximum; client-side inflight window. *(policy)*
- P1 Auto topic-alias management (negotiated, optimized).

### C.3 Consumption & routing
- P0 **Topic router**: wildcard + path params (`sensors/{deviceId}/temp`), powered by MQTT5 subscription identifiers. *(`ITopicRouter`)*
- P0 Typed handlers: `client.On<T>("topic/{id}", handler)`.
- P0 Stream consumption: `IAsyncEnumerable<MqttMessage<T>>` and `ChannelReader<…>`. *(`IDeliveryPipeline`)*
- P0 Backpressure policies: bounded queue, drop-oldest/drop-new/block, per-route concurrency, per-key ordering.
- P1 Rx-friendly `IObservable` adapter (separate package).
- P1 Shared-subscription "consumer group" helper (`$share/...`).

### C.4 Messaging & serialization
- P0 Pluggable `IMqttSerializer`; raw bytes default; JSON (source-gen) package. *(`IMqttSerializer`)*
- P0 Honor MQTT5 Content-Type + Payload-Format-Indicator round-trip.
- P0 Typed publish: `client.Publish("t", payloadObject, qos)`.
- P1 MessagePack + Protobuf serializer packages. ✅
- P0 **Request/Response (RPC)** helper: `RequestAsync<TReq,TResp>` managing response-topic, correlation-data, timeout, cleanup.

### C.5 Hosting, DI, configuration
- P0 `services.AddMqttClient("name", b => …)`; **named clients** (`IHttpClientFactory` pattern).
- P0 `IOptions<MqttClientOptions>` binding from `appsettings.json`.
- P0 `IHostedService` lifecycle (start/stop with the host).
- P0 `IHealthCheck` (connected? queue depth threshold?).
- P0 Standalone builder for non-DI use (same object graph).

### C.6 Observability
- P0 `ILogger` structured logs with scopes (clientId, packetId).
- P0 `ActivitySource` spans (connect/publish/subscribe/receive) with `messaging.*` OTel conventions.
- P0 `Meter` counters/gauges: connection state, inflight, queue depth, publish/receive throughput, reconnect count, RPC latency.

### C.7 Transports
- P0 TCP + TLS 1.2/1.3, client certificates, SNI. *(`IMqttTransport`)*
- P0 WebSocket (+ over HTTP proxy).
- P1 MQTT-over-QUIC.
- P1 Documented Blazor WASM (browser WebSocket) path + sample.
- P0 In-memory loopback transport (for tests).

### C.8 Security / auth
- P0 Username/password, TLS client cert.
- P0 `IMqttCredentialsProvider` (rotating tokens, e.g. SAS/OAuth).
- P1 MQTT5 enhanced auth (AUTH packet) with pluggable `IMqttEnhancedAuthenticator` (SCRAM/OAuth re-auth).

### C.9 Testing & tooling
- P0 In-memory test server + loopback transport; `FakeTimeProvider` integration; deterministic.
- P0 Assertion helpers (`ShouldHaveReceived`, etc.).
- P1 Analyzer that flags common misuse (unawaited publish, missing CT).

---

# PART D — The Swappability Contract (the core requirement)

The user must be able to replace any major part with a one-liner, without forking. The mechanism is three things working together:

1. **One small interface per capability** (§B.3).
2. **Defaults registered with `TryAdd*`** so a user registration *always* wins; or overridden fluently via the builder's `Use*`/`Configure*` methods.
3. **Named/keyed services** so multiple clients with different swaps coexist.

## D.1 Registration model

```csharp
services.AddMqttClient("telemetry", b =>
{
    b.UseOptions(o =>
    {
        o.Host = "broker.example.com";
        o.Port = 8883;
        o.ProtocolVersion = MqttProtocolVersion.V500;
        o.Tls.Enabled = true;
    });

    // Each Use*/Configure* below is OPTIONAL — omit to get the default.
    b.UseReconnectStrategy(/* … */);   // default: BackoffReconnectStrategy
    b.UseSerializer(/* … */);          // default: RawSerializer
    b.UseMessageStore(/* … */);        // default: InMemoryMessageStore
    b.UseTransport(/* … */);           // default: TcpTransport
    b.ConfigureBackpressure(p => p.BoundedCapacity = 1024);
});
```

Internally `AddMqttClient` calls `TryAddKeyed*` for every default keyed on the client name; `Use*` registers an override keyed on the same name. Resolution is per-named-client, so `"telemetry"` can use Polly while `"commands"` uses the default — no conflict.

Non-DI consumers get the identical graph:

```csharp
var client = new MqttClientBuilder("telemetry")
    .UseOptions(o => { o.Host = "…"; })
    .UseReconnectStrategy(new PollyReconnectStrategy(pipeline))
    .Build();
```

## D.2 Worked example — **auto-connect → Polly pipeline**

The reconnect strategy *owns the loop*, so it can be fully replaced. Contract:

```csharp
public delegate Task ConnectOnceAsync(CancellationToken ct);

public interface IReconnectStrategy
{
    /// Drives (re)connection. Calls connectOnce repeatedly per its own policy
    /// until success, give-up, or cancellation. Reports each attempt via context.
    Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken ct);
}
```

**Default** (`Pulse.Mqtt.Core`): exponential backoff + jitter, capped, infinite by default, uses `TimeProvider`.

**Swap** (`Pulse.Mqtt.Resilience.Polly`):

```csharp
public sealed class PollyReconnectStrategy : IReconnectStrategy
{
    private readonly ResiliencePipeline _pipeline;
    public PollyReconnectStrategy(ResiliencePipeline pipeline) => _pipeline = pipeline;

    public Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext ctx, CancellationToken ct) =>
        _pipeline.ExecuteAsync(async token => await connectOnce(token), ct).AsTask();
}
```

Registration — the *only* change the user makes:

```csharp
b.UseReconnectStrategy(_ => new PollyReconnectStrategy(
    new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            MaxRetryAttempts = int.MaxValue
        })
        .Build()));
```

Everything else (resubscribe, offline-queue flush, state events) is wired by the engine *around* the strategy and is unaffected. To disable reconnect entirely: `b.UseReconnectStrategy(new NoReconnectStrategy())`.

## D.3 Worked example — logging swap

The library logs through `ILogger<T>` only. So "swap logging" is just standard MEL configuration — Serilog, NLog, OTel logs, or `NullLogger` all work with zero library-specific code:

```csharp
// Serilog
services.AddLogging(b => b.AddSerilog());
// or silence it entirely
services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
```

High-frequency internal events go through `IMqttDiagnostics` (default forwards to `ILogger` with compile-time `LoggerMessage` source-gen; swap to a no-op for max throughput): `b.UseDiagnostics(MqttDiagnostics.Null);`

## D.4 Worked example — serialization swap

```csharp
// default = raw bytes. Swap to JSON (source-gen), per client:
b.UseSerializer(new JsonMqttSerializer(MyJsonContext.Default));
// or MessagePack:
b.UseSerializer(new MessagePackMqttSerializer(options));
```

`IMqttSerializer<T>` is resolved when you call `Publish<T>`/`On<T>`; the resolved serializer also sets the MQTT5 Content-Type so the round-trip is honest.

## D.5 What this buys us

- Users replace **auto-connect, retry, transport, serializer, offline store, session store, router, backpressure, logging, tracing, auth, clock** independently.
- We test the engine against *fakes* of each contract.
- Add-ons ship as separate packages; Core stays dependency-free and AOT-clean.

---

# PART E — Development Phases

Each phase states **Implements** (what becomes true), its **Steps**, each step's **Tasks**, and a **DoD** at every level. The generic DoD table at the top of this document applies; phase/step/task lines below add the *specific* acceptance criteria.

## Phase 0 — Foundations & scaffolding
**Implements:** a building, testing, benchmarking, CI-ready solution with frozen conventions.

- **Step 0.1 — Solution & projects**
  - Task: create `Pulse.Mqtt.sln`; projects `Pulse.Mqtt.Core`, `Pulse.Mqtt.Client`, `Pulse.Mqtt.DependencyInjection`, `Pulse.Mqtt.Testing`, test projects. **DoD:** `dotnet build` clean; multi-target `net10.0;net8.0`.
  - Task: `Directory.Build.props` — `Nullable=enable`, `TreatWarningsAsErrors=true`, analyzers, `IsAotCompatible=true`, `IsTrimmable=true`, deterministic builds, SourceLink. **DoD:** a deliberate nullable violation fails the build.
  - Task: central package management (`Directory.Packages.props`). **DoD:** versions pinned in one place.
- **Step 0.2 — CI & quality gates**
  - Task: CI runs build + test + pack on `net10.0`/`net8.0`. **DoD:** green pipeline on an empty test.
  - Task: AOT-publish smoke project + trim-analysis job. **DoD:** `publish -p:PublishAot=true` produces **zero** trim/AOT warnings.
  - Task: BenchmarkDotNet harness project (not in CI gate yet). **DoD:** `dotnet run -c Release` executes a no-op benchmark.
- **Step 0.3 — Shared primitives**
  - Task: `Result<T>`/`AppError`, `MqttException` hierarchy, `MqttReasonCode` enum. **DoD:** unit-tested; XML-documented.
- **Phase DoD:** repo is "clone → build → test → bench → AOT-publish" in one command each; conventions documented in `CONTRIBUTING.md`.

## Phase 1 — Protocol core: packet model & Span/Pipelines codec
**Implements:** correct, allocation-light encode/decode of all MQTT 3.1.1 + 5.0 control packets.

- **Step 1.1 — Packet model**
  - Task: immutable `record`/`readonly struct` types for every control packet + MQTT5 properties; `MqttApplicationMessage`. **DoD:** each type round-trips conceptually; no `byte[]` copies where `ReadOnlyMemory` suffices.
- **Step 1.2 — Decoder (`IMqttPacketCodec.Read`)**
  - Task: variable-length integer, UTF-8 string, binary, property bag readers over `ref SequenceReader<byte>`. **DoD:** fuzz tests for malformed/partial input never throw uncontrolled; partial frames yield "need more data".
  - Task: full decode for CONNECT…AUTH. **DoD:** decode vectors from the MQTT 5 spec pass.
- **Step 1.3 — Encoder (`IMqttPacketCodec.Write`)**
  - Task: write into `IBufferWriter<byte>` with pooled buffers; remaining-length precomputation. **DoD:** encode→decode identity tests for all packets; **0 bytes allocated** on the hot publish path (BenchmarkDotNet `[MemoryDiagnoser]`).
- **Step 1.4 — Conformance corpus**
  - Task: golden binary vectors (captured from a reference broker) under test. **DoD:** byte-exact match for representative packets.
- **Phase DoD:** `SpanPacketCodec` encodes/decodes every packet type for v3.1.1 and v5; property-based round-trip suite green; zero-alloc publish-encode benchmark recorded.

## Phase 2 — Transport abstraction + TCP/TLS
**Implements:** byte pipes over the network behind a swappable transport.

- **Step 2.1 — `IMqttTransport` / `IMqttTransportFactory`**
  - Task: contract exposing `PipeReader Input`, `PipeWriter Output`, `ConnectAsync`, `DisposeAsync`, negotiated features. **DoD:** documented lifecycle; cancellation honored.
- **Step 2.2 — `TcpTransport`**
  - Task: socket + `SslStream` (TLS 1.2/1.3), client certs, SNI, configurable validation. **DoD:** connects to a real broker (Mosquitto Testcontainer) over TCP and TLS; cancellation closes cleanly.
- **Step 2.3 — In-memory loopback transport** (enables Phase 11 early)
  - Task: a `DuplexPipe`-pair transport with injectable latency/drops. **DoD:** two endpoints exchange bytes deterministically under `FakeTimeProvider`.
- **Phase DoD:** any codec packet flows over TCP, TLS, and loopback transports; swapping transport is a one-liner; integration test against Mosquitto green.

## Phase 3 — Connection engine (raw client)
**Implements:** a working *non-resilient* client: connect, keepalive, pub/sub, QoS state machines, session state.

- **Step 3.1 — Handshake & keepalive**
  - Task: CONNECT→CONNACK negotiation (caps, session-present), PINGREQ scheduling via `TimeProvider`. **DoD:** keepalive proven with a fake clock; server-cap negotiation respected.
- **Step 3.2 — Receive loop & send path**
  - Task: single reader loop draining `PipeReader` → decode → hand to delivery sink; bounded writer. **DoD:** a slow sink never blocks the reader (drops to bounded queue, asserted).
- **Step 3.3 — QoS state machines**
  - Task: QoS1 (PUBACK) and QoS2 (PUBREC/PUBREL/PUBCOMP) inflight tracking via `ISessionStore`; packet-id allocation. **DoD:** duplicate-delivery and retransmit-on-reconnect covered by tests; ids never leak.
- **Step 3.4 — Subscribe/unsubscribe**
  - Task: SUBSCRIBE/SUBACK with all options; UNSUBSCRIBE. **DoD:** reason codes surfaced; per-filter granted-QoS reported.
- **Phase DoD:** `RawMqttClient` does a full connect → subscribe → publish (all QoS) → receive → disconnect against Mosquitto; session store holds inflight state; everything cancellable.

## Phase 4 — Resilience layer
**Implements:** the *one always-resilient client* — reconnect, resubscribe, offline queue — all swappable.

- **Step 4.1 — `IReconnectStrategy` + default backoff**
  - Task: strategy owns the loop (§D.2); default exp+jitter. **DoD:** reconnect proven on simulated drops via loopback transport + fake clock.
- **Step 4.2 — Resubscribe & session restore**
  - Task: on reconnect with session-present=false, replay subscriptions from `ISessionStore`; restore inflight. **DoD:** subscriptions survive a forced drop; no duplicate subscribes when session-present=true.
- **Step 4.3 — Offline outbound queue (`IMessageStore`)**
  - Task: bounded `InMemoryMessageStore`; publishes while disconnected enqueue and flush on reconnect in order. **DoD:** ordering + bound enforced; overflow policy (block/drop) tested.
- **Step 4.4 — `ConnectionState` stream**
  - Task: current value + change stream (`IAsyncEnumerable`/event). **DoD:** transitions observed in correct order under reconnect.
- **Step 4.5 — Polly add-on**
  - Task: `Pulse.Mqtt.Resilience.Polly` with `PollyReconnectStrategy`. **DoD:** swapping to Polly passes the *same* Step 4.1 acceptance test unchanged.
- **Phase DoD:** kill the broker mid-run → client reconnects, resubscribes, flushes queued publishes, emits correct state events; default and Polly strategies both pass; offline store swappable.

## Phase 5 — Consumption & routing
**Implements:** topic routing, typed handlers, stream consumption, backpressure — the headline DX.

- **Step 5.1 — `ITopicRouter` (trie + sub-ids)**
  - Task: wildcard + `{param}` matching; map MQTT5 subscription identifiers → routes for O(1) dispatch. **DoD:** `+`/`#`/param/overlapping-route matching table tested; sub-id fast path verified.
- **Step 5.2 — `IDeliveryPipeline` (Channels)**
  - Task: bounded channel dispatch; policies: drop-oldest/new/block, per-route `MaxConcurrency`, per-key ordering. **DoD:** backpressure policies each have a dedicated test; reader loop never blocked.
- **Step 5.3 — Handler & stream APIs**
  - Task: `On("t/{id}", handler)`, `On<T>(…)`, `Subscribe<T>(…) : IAsyncEnumerable`, `ChannelReader` access. **DoD:** `await foreach` consumption sample works; route params delivered.
- **Phase DoD:** a sample routes three overlapping topics to typed handlers with distinct backpressure policies; slow handler on route A never affects route B; delivery pipeline swappable to a Dataflow impl.

## Phase 6 — Serialization & typed messaging
**Implements:** pluggable payload (de)serialization with honest MQTT5 content metadata.

- **Step 6.1 — `IMqttSerializer` + raw default + content metadata**
  - Task: contract; `RawSerializer`; set/read Content-Type + Payload-Format-Indicator. **DoD:** round-trip preserves metadata.
- **Step 6.2 — `Pulse.Mqtt.Serialization.Json` (source-gen)**
  - Task: `JsonMqttSerializer` using `JsonSerializerContext`. **DoD:** AOT-clean; typed publish/receive round-trips a record.
- **Step 6.3 — Typed publish/receive wiring**
  - Task: `Publish<T>`, `On<T>`, `Subscribe<T>` resolve the serializer. **DoD:** serializer swap (JSON↔MessagePack) is one line and changes nothing else.
- **Phase DoD:** typed end-to-end over a real broker with JSON; MessagePack package passes the same contract tests; raw default still available.

## Phase 7 — DI, hosting, configuration
**Implements:** `AddMqttClient`, named clients, options binding, hosted lifecycle, health checks.

- **Step 7.1 — `AddMqttClient` + builder + keyed defaults**
  - Task: `TryAddKeyed*` defaults; `Use*` overrides; per-name resolution. **DoD:** two named clients with *different* swaps run side by side in one test host.
- **Step 7.2 — Options binding**
  - Task: `IOptions<MqttClientOptions>` bound from config + validation (`ValidateOnStart`). **DoD:** invalid config fails fast at startup with a clear message.
- **Step 7.3 — Hosted lifecycle + health**
  - Task: `IHostedService` start/stop; `IHealthCheck` (connected + queue-depth threshold). **DoD:** host shutdown disconnects gracefully within timeout; health flips on disconnect.
- **Phase DoD:** a `Microsoft.Extensions.Hosting` worker sample boots two named clients from `appsettings.json`, exposes health, and shuts down cleanly.

## Phase 8 — Observability
**Implements:** logs, traces, metrics by default.

- **Step 8.1 — Structured logging**
  - Task: `LoggerMessage` source-gen events with scopes (clientId, packetId, reconnect attempt). **DoD:** no string interpolation on hot paths; allocation-checked.
- **Step 8.2 — Tracing**
  - Task: `ActivitySource` spans for connect/publish/subscribe/receive with `messaging.*` attributes; context propagation via user properties (opt-in). **DoD:** spans visible in an OTel console exporter sample.
- **Step 8.3 — Metrics**
  - Task: `Meter` instruments (connection state, inflight, queue depth, throughput, reconnects, RPC latency). **DoD:** counters/gauges observed via `MeterListener` test.
- **Phase DoD:** `Pulse.Mqtt.Diagnostics` add-on lights up traces+metrics with one registration; instrumentation is no-op when the add-on is absent (zero overhead asserted).

## Phase 9 — Higher-level patterns
**Implements:** RPC, shared subscriptions, enhanced auth, flow control, topic-alias.

- **Step 9.1 — Request/Response (RPC)**
  - Task: `RequestAsync<TReq,TResp>` managing response-topic, correlation-data, timeout, and subscription cleanup. **DoD:** concurrent RPCs don't cross responses; timeout cancels and cleans up.
- **Step 9.2 — Flow control & topic-alias**
  - Task: honor server Receive-Maximum (inflight window); auto-allocate/optimize outbound topic aliases. **DoD:** inflight never exceeds negotiated max; alias reduces bytes (benchmark).
- **Step 9.3 — Shared subscriptions & enhanced auth**
  - Task: `$share/...` helper; `IMqttEnhancedAuthenticator` AUTH-packet exchange hook. **DoD:** shared sub load-balances across two clients in a broker test; a sample SCRAM authenticator completes the exchange.
- **Phase DoD:** RPC, flow control, topic-alias, shared subs demonstrated against EMQX/Mosquitto; enhanced auth is pluggable and off by default.

## Phase 10 — Additional transports
**Implements:** WebSocket, QUIC, Blazor-WASM path.

- **Step 10.1 — WebSocket transport** — Task: `ClientWebSocket` + proxy support. **DoD:** connects to a broker WS listener; passes the transport contract suite.
- **Step 10.2 — QUIC transport** — Task: `System.Net.Quic`. **DoD:** connects to a QUIC-enabled broker; graceful fallback documented when unavailable.
- **Step 10.3 — Blazor WASM** — Task: browser WebSocket sample + trimming validation. **DoD:** sample publishes/subscribes from a WASM app.
- **Phase DoD:** all four transports (TCP/WS/QUIC/loopback) satisfy one shared transport contract test; transport swap remains a one-liner.

## Phase 11 — Testing harness
**Implements:** fast, broker-free testing for *consumers* of the library.

- **Step 11.1 — In-memory test server** — Task: minimal broker over loopback (connect/sub/pub/QoS routing) + `FakeTimeProvider`. **DoD:** a consumer test does connect→sub→pub→assert with no external process, deterministic.
- **Step 11.2 — Assertions & fakes** — Task: `ShouldHaveReceived`, fake stores/serializers. **DoD:** documented, used by our own suite.
- **Phase DoD:** `Pulse.Mqtt.Testing` lets a downstream app test routing/handlers in milliseconds; our own integration tests reuse it.

## Phase 12 — Performance hardening
**Implements:** verified throughput/latency/allocation targets and AOT cleanliness.

- **Step 12.1 — Benchmark suite** — Task: encode/decode, publish throughput, end-to-end latency, allocation per op. **DoD:** baseline numbers recorded and committed.
- **Step 12.2 — Hot-path tuning** — Task: pooling, `ConfigureAwait(false)`, struct enumerators, remove avoidable allocations. **DoD:** publish-encode and receive-decode at **0 alloc**; throughput target met (set after first baseline).
- **Step 12.3 — AOT/trim gate** — Task: promote AOT-publish job to a required gate. **DoD:** zero AOT/trim warnings is enforced in CI.
- **Phase DoD:** benchmarks meet agreed targets; regressions fail CI via a benchmark threshold; AOT gate required.

## Phase 13 — Docs, samples, release
**Implements:** a publishable, learnable v1.

- **Step 13.1 — Docs** — Task: conceptual guide, API reference (DocFX), the swap recipes from Part D, MQTTnet migration guide. **DoD:** every public type has XML docs; quickstart works copy-paste.
- **Step 13.2 — Samples** — Task: console, worker (DI), Blazor WASM, RPC, Polly-swap, custom-transport. **DoD:** each sample builds and runs in CI.
- **Step 13.3 — Packaging & versioning** — Task: NuGet metadata, symbols, README per package, semver policy doc, public-API-diff guard (`PublicApiAnalyzers`). **DoD:** `dotnet pack` produces all packages; an accidental public-API change fails the build.
- **Phase DoD:** v1.0.0 packages publishable; `Pulse.Mqtt.Core` public surface declared frozen; CHANGELOG and migration guide complete.

---

# PART F — Quality Gates

## F.1 Testing strategy
- **Unit (xUnit + Shouldly):** codec round-trips, QoS state machines, router matching, backpressure policies, reconnect loop (fake clock), serializer round-trips. Prefer **fakes** of contracts over mocks.
- **Property/fuzz:** decoder never throws uncontrolled on malformed input.
- **Integration (Testcontainers):** Mosquitto + EMQX images for real protocol behavior, TLS, shared subs, QUIC.
- **Deterministic:** `FakeTimeProvider` everywhere time matters; explicit timeouts; no global state.
- **Consumer-facing:** `Pulse.Mqtt.Testing` proves the library is testable without a broker.
- **Regression:** every bug gets a failing test first.

## F.2 Performance gates
- Zero allocation on publish-encode and receive-decode hot paths (`[MemoryDiagnoser]` enforced).
- BenchmarkDotNet thresholds in CI; regressions fail.
- AOT-publish with **zero** trim/AOT warnings is a required check.

## F.3 Versioning & stability
- Strict semver. `Pulse.Mqtt.Core` abstractions frozen at v1.0; changes require a major bump.
- `Microsoft.CodeAnalysis.PublicApiAnalyzers` guards the public surface (`PublicAPI.Shipped/Unshipped.txt`).
- Add-ons evolve independently; Core/Client never depend on them.

## F.4 Risks & mitigations
| Risk | Mitigation |
|---|---|
| QUIC broker support is uneven | Ship QUIC as opt-in (P1); document fallback to TCP/WS. |
| AOT/trim regressions creep in | AOT gate required from Phase 12; no reflection on hot paths. |
| Scope creep toward a broker | Test server only; broker explicitly out of v1 scope. |
| API churn repeats MQTTnet's mistake | Frozen Core contract + public-API-diff guard + semver. |
| Serializer/persistence lock-in | All behind contracts in add-on packages; raw/in-memory defaults in Core. |
| Backpressure surprises under load | Bounded everything; per-policy tests; slow-consumer isolation proven in Phase 5. |

---

## Appendix — Glossary
- **Swap point:** the interface (§B.3) a user replaces to change a behavior.
- **Named client:** a configured client resolved by string name (`IHttpClientFactory` pattern), each with independent swaps.
- **Delivery pipeline:** the bounded dispatch layer between the protocol receive loop and user handlers (where backpressure lives).
- **Reconnect strategy:** owns the (re)connect loop; default = backoff, swappable to Polly or none.
