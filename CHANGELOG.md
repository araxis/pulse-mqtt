# Changelog

## 0.6.0 (unreleased)

- Observability completion: `connect` (Client) and `receive` (Consumer) spans join the existing
  `publish` span; new `connect.duration` and `publish.duration` histograms (seconds) and
  `offline.queue.depth` / `offline.queue.dropped` observable instruments round out the metrics.
  All stay nearly free with no listener attached.
- Trace context propagation: opt in with `ResilientMqttClientOptions.PropagateTraceContext` and the
  active span's W3C `traceparent`/`tracestate` rides on each publish's user properties; the
  `receive` span always honors an incoming `traceparent`, so a producer's span and a consumer's
  handler join one distributed trace across the broker. Off by default.
- Broker interop matrix: a shared `BrokerScenarios` conformance suite now runs the same scenarios
  — handshake, QoS 0/1/2 round trips, retained messages, shared subscriptions, 64 KB payloads,
  and persistent-session resume — against **Mosquitto 2, EMQX 5.8, and HiveMQ CE 2024.3** as
  Testcontainers images, so a failure names the broker and the capability. EMQX and HiveMQ carry
  a `BrokerMatrix` category and run on `main` (and on demand) via `broker-matrix.yml`; every PR
  still runs the Mosquitto suite. A [compatibility table](docs/reference/broker-compatibility.md)
  documents the brokers, versions, and verified scenarios.

## 0.5.0

- In-flight redelivery on session resume: with a persistent session (`CleanStart = false`)
  and a broker that preserves it, unacknowledged QoS 1/2 exchanges retransmit in order after a
  reconnect — PUBLISH packets with the DUP flag and their original identifiers, PUBREL alone
  once PUBREC was received — before the offline queue flushes. Inbound QoS 2 duplicate
  suppression survives the reconnect, and a fresh session discards the state per spec. The
  tracked state lives behind `ISessionStore` (`SaveInFlightAsync`/`LoadInFlightAsync`) so
  durable stores carry it across restarts; clean-start clients skip the tracking entirely. A
  publish interrupted mid-exchange returns the new `PublishDisposition.InFlight`.
- Enhanced authentication: an `IMqttAuthenticator` swap point drives the MQTT 5 AUTH
  exchange — method and initial data on CONNECT, challenge/response rounds until the broker's
  CONNACK, and client-initiated re-authentication on a live connection via
  `ReAuthenticateAsync`. With none configured, no AUTH is ever sent.
- Presence: first-class last-will configuration (options, configuration binding, and the
  fluent builder — text, bytes, typed, and full v5 forms) plus a will **factory** invoked on
  every connection attempt for fresh topics and payloads. A **birth message** — the will's
  "online" mirror — publishes automatically on every connection-up, after re-subscription and
  before the offline queue flushes, in static, typed, and per-attempt factory forms, with a
  configurable failure policy. The retained online/offline cycle is verified end to end
  against a real broker.

## 0.4.0

- Receive-maximum flow control: outbound QoS 1/2 publishes never exceed the broker's CONNACK
  `ReceiveMaximum`. Excess publishers wait — bounded and cancellable — until an
  acknowledgement frees a slot (PUBACK for QoS 1, PUBREC for QoS 2, per the specification).
  The limit re-arms per connection; brokers that advertise none cost the publish path
  nothing.
- Topic aliases: inbound resolution is automatic once the CONNECT advertises a
  `TopicAliasMaximum` (violations fault the session per the specification); outbound
  compression is opt-in via `RawMqttClientOptions.UseOutboundTopicAliases`, first come, first
  served within the broker's maximum, reset on every reconnect. The alias-only publish shape
  encodes on the zero-allocation fast path.

## 0.3.0

- Broker-initiated DISCONNECT handling: the reason code, reason string, and server reference
  surface through `MqttServerDisconnectedException`, the state stream, the lifecycle
  down-context, and a dedicated log event. Terminal reasons (`NotAuthorized`, `Banned`,
  `SessionTakenOver`, `ServerMoved`, `UseAnotherServer`) fault sticky instead of reconnecting;
  the classification is swappable through `IReconnectDecision`.
- `IConnectionLifecycle.OnConnectionDownAsync` now receives an `IConnectionDownContext`
  (reason, reason string, server reference, error) instead of a bare reason code.
- The `MqttReasonCode` enum gains the remaining MQTT 5 codes (topic/payload validation,
  rate and connection limits, redirects, feature-support indicators).
- Maximum-packet-size compliance: when the broker's CONNACK advertises a limit, every
  outbound packet is size-checked before any byte reaches the wire; oversized ones fail fast
  with `MqttPacketTooLargeException` instead of getting the client disconnected. A queued
  publish that exceeds a stricter reconnected broker's limit is dropped loudly (logged and
  counted) rather than poisoning the flush.
- Builds on the GA .NET 10 SDK; dependency pins refreshed.

## 0.2.0

- Fluent API: `PulseMqttClientBuilder` for direct construction, and chainable
  `Publish`/`Route`/`Request` builders over the regular client operations.

## 0.1.0

Initial release.

- Complete MQTT 3.1.1 + 5.0 wire codec: all 15 control packets, full v5 property sets,
  span-based and allocation-light, fuzz-hardened framing.
- Transports behind one contract: TCP with TLS 1.2/1.3 (client certificates, SNI), WebSocket,
  and an in-memory loopback.
- Raw client: handshake, keep-alive, QoS 0/1/2 state machines, subscriptions — verified against
  a real Mosquitto broker.
- Resilient client: background connect, exponential-backoff reconnect (swappable, Polly add-on
  included), automatic re-subscription before queued publishes flush, bounded offline queue with
  explicit overflow policies, sticky faults on terminal failures, observable connection state.
- Topic routing with named parameters (`sensors/{id}/temp`), per-route bounded queues and
  concurrency, handler fault isolation, and stream consumption.
- Typed messaging through a pluggable serializer; source-generated JSON implementation.
- Request/response over MQTT 5 response topics and correlation data, caller and responder sides.
- Dependency injection: named clients, per-client behavior swaps, hosted lifecycle, health checks.
- Observability: structured logs, `Pulse.Mqtt` activity source and meter.
- `PulseMqttTestBroker`: an in-process broker for fast, deterministic consumer tests.
- Native AOT verified: zero trim/AOT warnings, full-stack native smoke binary.
- Host-managed or manual client lifecycle: `StartWithHost = false` hands start, stop, and
  restart to the application.
- Sample application covering hosting, routing, typed messaging, and request/response.
- Continuous integration and tag-driven NuGet release pipelines.
