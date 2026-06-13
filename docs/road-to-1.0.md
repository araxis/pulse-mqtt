# Road to 1.0

What 1.0 means for this library, every feature it requires, the nice-to-haves around it, and a
testable definition of done for each step. Items marked **gap** were verified against the
current source, not assumed.

## What 1.0 promises

1. **Spec-complete client behavior** for MQTT 5.0 and 3.1.1 — not just packet encoding, but the
   negotiated-limit and lifecycle behaviors a broker expects from a conforming client.
2. **A frozen public API** under semantic versioning: no breaking changes until 2.0.
3. **Proven interop** against the major brokers, not one.
4. **The existing quality gates stay green**: zero build warnings, Native AOT with zero
   trim/AOT warnings, benchmarks at or better than current numbers, all tests deterministic.

Current state (0.2.0): full v5 + 3.1.1 codec, TCP/TLS/WebSocket/loopback transports, raw and
resilient clients, routing, typed messaging, RPC, DI, health checks, observability, the
in-process test broker, fluent API, docs site, CI with trusted publishing and
prerelease-on-merge. v5 subscription options (NoLocal, RetainAsPublished, RetainHandling) are
already complete.

---

## Required for 1.0

### F1 — Broker-initiated DISCONNECT handling (gap)

The connection layer currently ignores an inbound DISCONNECT; the client only notices the
socket closing. A conforming client must treat DISCONNECT as an orderly close and surface the
broker's reason.

**Definition of done**
- [ ] An inbound DISCONNECT closes the session without treating it as a protocol error.
- [ ] The reason code and reason string reach the supervisor: the `ConnectionStateChanged`
      transition and `OnConnectionDownAsync` carry them; the state log includes them.
- [ ] Reason codes that indicate a permanent condition (for example `NotAuthorized`,
      `ServerMoved`, `UseAnotherServer`) flow through `IReconnectDecision` so the default
      faults instead of hammering; transient reasons reconnect as usual.
- [ ] `ServerReference` from DISCONNECT (and CONNACK) is exposed on the down-context for
      redirect-aware deployments.
- [ ] Unit tests cover: graceful disconnect mid-traffic, disconnect during an in-flight QoS 2
      exchange, disconnect with each terminal reason; an integration test provokes a real
      broker disconnect (for example keep-alive violation).

### F2 — Receive Maximum flow control, outbound (gap)

The broker's CONNACK `ReceiveMaximum` caps how many QoS > 0 publishes may be unacknowledged at
once. The client currently bounds in-flight work only by packet-identifier availability.

**Definition of done**
- [ ] Outbound QoS 1/2 publishes never exceed the broker's `ReceiveMaximum`; excess callers
      wait (bounded, cancellable) rather than erroring.
- [ ] The limit re-arms per connection from each CONNACK; a reconnect to a broker with a
      different limit honors the new one.
- [ ] No throughput regression at the default Mosquitto limit (sustained-throughput benchmark
      within noise of current numbers).
- [ ] Unit tests with a scripted broker prove: the (N+1)th publish waits until an ack frees a
      slot; cancellation while waiting releases cleanly; QoS 0 is unaffected.

### F3 — Maximum Packet Size compliance, outbound (gap)

The broker's CONNACK `MaximumPacketSize` bounds what the client may send. Today nothing
enforces it, which can get the client disconnected with `PacketTooLarge`.

**Definition of done**
- [ ] A publish whose encoded size exceeds the negotiated limit fails fast with a clear,
      typed error before any bytes hit the wire — never a broker disconnect.
- [ ] The check accounts for the full encoded packet (header, properties, payload), not just
      the payload length.
- [ ] The client-side `MaximumPacketSize` request in CONNECT remains honored for inbound
      (oversized inbound already faults via the framing limit).
- [ ] Unit tests cover: exactly at the limit (sent), one byte over (rejected), no limit
      advertised (unlimited up to the protocol maximum).

### F4 — Topic aliases (gap)

Bandwidth saver for repeated topics, both directions. Inbound resolution is mandatory the
moment the client advertises a `TopicAliasMaximum`; outbound use is bounded by the broker's.

**Definition of done**
- [ ] Inbound: when the client advertises `TopicAliasMaximum`, publishes carrying an alias
      with an empty topic resolve to the registered topic; an unknown or out-of-range alias
      is a protocol error per the specification.
- [ ] Outbound: an opt-in policy assigns aliases to the hottest topics within the broker's
      advertised maximum; once aliased, repeat publishes send the alias and an empty topic.
- [ ] Aliases reset per connection (both directions) — nothing leaks across reconnects.
- [ ] Off by default outbound; one option enables it. Inbound resolution is automatic.
- [ ] Unit tests cover registration, reuse, reset on reconnect, and the two inbound error
      cases; a benchmark shows the bytes-per-message win on a repeated-topic workload.

### F5 — In-flight redelivery on session resume (gap)

With `CleanStart = false` and a broker that preserved the session, the spec requires the
client to retransmit unacknowledged QoS 1/2 publishes (DUP set) and unacknowledged PUBRELs.
Today in-flight operations fail on connection loss and only never-sent messages flush from the
offline queue.

**Definition of done**
- [ ] Unacknowledged QoS 1/2 PUBLISH and PUBREL packets persist in session state and
      retransmit, in original order, before the offline queue flushes, when the broker
      reports `SessionPresent`.
- [ ] Retransmitted PUBLISH packets carry DUP; packet identifiers are reused correctly and
      not returned to the allocator until the exchange completes.
- [ ] When the broker did **not** preserve the session, in-flight state is discarded and the
      awaiting callers fail with a clear error (current behavior, now explicit and tested).
- [ ] Inbound QoS 2 duplicate suppression state survives the reconnect for the same session.
- [ ] The state lives behind the existing `ISessionStore` swap point so durable stores can
      persist it; the in-memory default keeps it for the process.
- [ ] Scripted-broker tests cover: drop after PUBLISH/before PUBACK, drop between PUBREC and
      PUBREL, drop after PUBREL/before PUBCOMP — each resumes to completion; an integration
      test proves QoS 2 end-to-end across a real reconnect with a persistent session.

### F6 — Enhanced authentication (AUTH) (gap)

The v5 AUTH exchange (SCRAM, OAuth-style challenges, Kerberos). The packet codec exists; the
connection layer ignores AUTH packets.

**Definition of done**
- [ ] A new swap point (an authenticator contract) handles challenge/response: it sees each
      broker AUTH/CONNACK step for its method and produces the next AUTH packet until success
      or failure.
- [ ] The handshake supports `ReAuthenticate`-initiated exchanges on a live connection.
- [ ] No authenticator configured = exactly today's behavior (no AUTH sent, no overhead).
- [ ] Unit tests drive a scripted multi-step exchange to success and to rejection; rejection
      maps to a terminal connect failure through the existing decision flow.

### F11 — Presence: last-will and birth messages, static and factory forms

Both halves of device presence. The **last will** is the message the broker publishes on the
client's behalf when the connection dies ungracefully — it already encodes on CONNECT but is
reachable only through the raw packet today. The **birth message** (the "first will") is its
mirror: a message the client itself publishes automatically on every connection-up, announcing
it is online. Together with a retained topic they give the classic presence pattern:

```
status/device-7   ← birth publishes "online" (retained) on every connect
status/device-7   ← will publishes "offline" (retained) when the client drops
```

Static forms cover the common case; **factory forms** compute the topic and payload fresh for
each connection attempt — session counters, timestamps, current configuration — instead of
freezing them at startup.

**Definition of done**
- [ ] **Last will, static**: `PulseMqttClientOptions` (DI) and
      `PulseMqttClientBuilder.WithWill(...)` (fluent) express topic, payload, QoS, retain,
      will-delay interval, and the v5 will properties, without touching the raw packet.
- [ ] **Last will, factory**: a per-connection factory
      (`CancellationToken → ValueTask<MqttWillMessage>`) is invoked on **every** connection
      attempt before CONNECT is sent, so each reconnect can carry a fresh topic and payload.
      A throwing factory fails that attempt like any connect failure — classified by the
      reconnect decision, never swallowed.
- [ ] **Birth, static**: an announcement message (topic, payload, QoS, retain) the client
      publishes automatically on every connection-up — after re-subscription, before the
      offline queue flushes, before the state becomes `Connected` — so observers never see
      "online" from a client whose session is not actually restored yet.
- [ ] **Birth, factory**: the same per-connection factory form, invoked at publish time with
      the connection attempt visible, so payloads can carry attempt counts or timestamps.
- [ ] Typed payload overloads for both, through the configured serializer.
- [ ] Birth publish failures are observable (logged, counted) and configurable: fail the
      connection-up (default — presence must be true) or log-and-continue.
- [ ] Both factories take the client's `TimeProvider` reality into account: deterministic in
      tests with a fake clock.
- [ ] Integration test against a real broker: subscriber sees retained "online" after
      connect, "offline" after an ungraceful drop (socket kill), and "online" again after
      the automatic reconnect — the full presence cycle without application code.
- [ ] Documented as a presence guide page with the retained online/offline pattern.

### F7 — API freeze and review ✅

**Definition of done**
- [x] `Microsoft.CodeAnalysis.PublicApiAnalyzers` is enabled on every shipped package with
      committed `PublicAPI.Shipped.txt` baselines; an unintended public-surface change fails
      the build (RS0016/RS0017, verified).
- [x] One documented API review pass over the whole surface (an adversarial multi-agent review):
      it found and the freeze fixed four issues — the orphaned `MqttApplicationMessage` duplicate
      removed, the internal `MqttPacketIdAllocator` and the SQLite code-sharing base made
      internal (the base now composed, so no `Microsoft.Data.Sqlite` type leaks into the surface),
      and `CancellationToken = default` made uniform across `ResilientMqttClient`.
- [x] XML documentation for 100% of public members (enforced by `GenerateDocumentationFile` +
      warnings-as-errors).
- [x] A `BREAKING-CHANGES.md` policy note states the semantic-versioning commitment.

### F8 — Broker interop matrix

Mosquitto alone proves too little for a 1.0 interop claim.

**Definition of done**
- [x] The Testcontainers integration suite runs the same scenario set against **Mosquitto 2,
      EMQX 5.8, and HiveMQ CE 2024.3**: handshake, QoS 0/1/2 round trips, persistent-session
      resume, retained messages, shared subscriptions, and large (64 KB) payloads. The shared
      `BrokerScenarios` helper drives every broker, so a failure names the broker and the
      scenario. (Receive-maximum (F2) and topic aliases (F4) keep their dedicated single-broker
      tests; TLS interop is tracked separately.)
- [x] `broker-matrix.yml` runs the EMQX/HiveMQ matrix on PRs that touch the source, the
      integration tests, or shared build inputs (gating before merge), and again on `main` and
      on demand; a failure on `main` opens a tracking issue. The fast `ci.yml` lane filters
      `Category!=BrokerMatrix`, so Mosquitto runs on every PR while the heavy images stay off
      unrelated PRs.
- [x] A [compatibility table](reference/broker-compatibility.md) in the docs lists each broker,
      the version tested, and the verified scenarios.

### F9 — Stable toolchain and targets

**Definition of done**
- [ ] `global.json` pins a GA .NET 10 SDK (the repo currently pins a release candidate);
      `allowPrerelease` is gone.
- [ ] Package targets remain `net8.0` + `net10.0` (both LTS); CI and release pipelines build
      on the GA SDK.
- [ ] All dependency pins reviewed and floated to current stable patch versions.

### F10 — Soak and stress validation

**Definition of done**
- [ ] A soak harness runs a client against a real broker for ≥ 24 hours with periodic broker
      restarts and network cuts: zero leaked tasks/sockets/memory growth (heap snapshot at
      starts and end within tolerance), zero lost QoS 1/2 messages, reconnect always recovers.
- [ ] A chaos test (random disconnects under sustained load via the scripted transport) runs
      in CI in bounded form (minutes, not hours) and passes deterministically.
- [ ] The benchmark suite runs on the release candidate and the results are within noise of —
      or better than — the published 0.2.x numbers; the comparison doc is refreshed.

---

## Nice-to-have for 1.0 (ship if ready, never block)

### N1 — Durable stores package (`Pulse.Mqtt.Storage.Sqlite`)

`ISessionStore` + `IMessageStore` over SQLite so subscriptions, the offline queue, and (with
F5) in-flight state survive restarts.

**DoD:** both contracts implemented including the incremental upsert/remove paths and the
peek/remove-head at-least-once contract; crash-mid-flush integration test re-sends rather than
loses; corruption test recovers with a clear error; AOT-safe (no reflection-based ORM).

### N2 — MessagePack serializer package (`Pulse.Mqtt.Serialization.MessagePack`) ✅

**DoD:** `IMqttSerializer` over MessagePack with correct content type; AOT-compatible
(generated resolvers, no dynamic codegen at runtime); round-trip and interop tests; documented
in Typed messaging.

Done in 0.7.0: `MessagePackMqttSerializer` takes `MessagePackSerializerOptions`, so a
source-generated resolver keeps it AOT-safe (the package builds with `IsAotCompatible`); content
type `application/x-msgpack`. Tests round-trip through the generated resolver, verify the bytes
are valid MessagePack (interop via `ConvertToJson`) and smaller than JSON, and check the error
path. Documented in Typed messaging.

### N3 — Observability completion ✅

**DoD:** a `receive` span (Consumer kind, linked to the producer context when the publish
carried trace propagation), a `connect` span around connection attempts, histogram instruments
for publish duration and connect duration, and gauges for offline-queue depth and dropped
count; docs updated; overhead measured and negligible with no listener.

Done in 0.6.0: `connect`/`receive` spans alongside `publish`; `connect.duration` and
`publish.duration` histograms (seconds); `offline.queue.depth` observable gauge and
`offline.queue.dropped` observable counter, one measurement per live client. Spans and
instruments are no-ops with no listener (`StartActivity` returns null). Documented in the
observability guide.

### N4 — Trace context propagation ✅

W3C `traceparent` in user properties, producer-side inject + consumer-side extract, opt-in.

**DoD:** a publish inside an active span produces a routed handler whose `Activity` is a child
across two clients; off by default; documented.

Done in 0.6.0: `ResilientMqttClientOptions.PropagateTraceContext` (off by default) injects the
active span's `traceparent`/`tracestate` onto outbound publishes; the `receive` span always
extracts an incoming `traceparent` and parents on it, so the routed handler's `Activity` is a
remote child of the producer's span. Verified by tests for inject, off-by-default, and remote
parenting.

### N5 — MQTTnet migration guide ✅

**DoD:** a docs page mapping every common MQTTnet pattern (factory/options/builders, handlers,
managed client behaviors) to the Pulse equivalent, with before/after code.

Done in 0.7.0: [Migrating from MQTTnet](guide/migrating-from-mqttnet.md) maps the factory/options
builders, the managed client (reconnect/queue/storage), message and topic-filter builders, the
received-message event, last will/birth, request/response, and TLS/WebSocket to their Pulse
equivalents, with before/after code, and is linked in the guide sidebar.

### N6 — `IAsyncEnumerable` request streaming ✅

Server-streamed RPC: one request, many correlated responses.

**DoD:** `RequestStreamAsync` yields responses until an end-of-stream marker or timeout;
responder-side helper publishes the marker; backpressure bounded; tests cover early consumer
abandonment.

Done in 0.7.0: `RequestStreamAsync` (raw + typed) yields correlated responses via a bounded
channel (`MqttRequestStreamOptions.Capacity`) until the end-of-stream user-property marker, the
per-item `IdleTimeout`, or cancellation; `OnRequestStreamAsync` publishes each yielded item and
the marker. Tests cover the happy path, the responder, early abandonment, and the idle timeout.

### N7 — WebSocket proxy and header options ✅

**DoD:** explicit proxy configuration and per-connect headers on `WebSocketTransportOptions`
(today reachable only via `ConfigureClient`); documented with a reverse-proxy example.

Done in 0.7.0: `WebSocketTransportOptions.Proxy` and `.Headers` apply to the opening handshake
before `ConfigureClient` (which still runs last and can override). Tests verify a custom header
reaches the server and that an unreachable proxy fails the connection; documented with a
reverse-proxy example.

---

## Milestones

| Version | Contents | Exit criterion |
| --- | --- | --- |
| **0.3.0** | F1 DISCONNECT, F3 max packet size, F9 GA toolchain | A broker can say no politely and the client behaves; builds on GA SDK |
| **0.4.0** | F2 receive maximum, F4 topic aliases | Negotiated-limit compliance complete |
| **0.5.0** | F11 presence (will + birth, static + factory), F5 session redelivery, F6 enhanced auth, N1 if ready | Persistent sessions and presence are honest end to end |
| **0.6.0** | F8 broker matrix, N3/N4 as ready | Interop proven beyond Mosquitto |
| **1.0.0-rc.1** | F7 API freeze, F10 soak/stress, docs complete | Public API locked; release candidate published |
| **1.0.0** | RC feedback only — no new features | All F-items checked; quality gates green |

Rules of the road, unchanged from the start of the project: every step lands as its own merged
PR with tests; quality, performance, memory, and swap-point interchangeability are
non-negotiable; anything that would break the frozen API after 1.0.0-rc.1 waits for 2.0.

The post-1.0 horizon (not planned here): MQTT over QUIC, a Protobuf serializer, broker-side
feature probes, and an analyzer package for common misuse.
