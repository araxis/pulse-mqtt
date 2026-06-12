# Changelog

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
