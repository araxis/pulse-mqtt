# Changelog

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
