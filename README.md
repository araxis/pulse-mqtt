# Pulse.Mqtt

A high-performance, resilient MQTT 5.0 / 3.1.1 client for modern .NET.

> Status: early development. Targets `net8.0` and `net10.0`.

## Why

Most .NET MQTT clients leave the resilient-connection layer — auto-reconnect, offline
queueing, re-subscription — to the application. Pulse makes that layer first-class and
**swappable**, alongside topic routing, typed messaging, dependency-injection integration,
and built-in observability, on a fast `System.IO.Pipelines` core.

Every major capability sits behind a small contract with a sensible default, so any part
can be replaced without forking — for example, swapping the built-in reconnect strategy for
a Polly resilience pipeline, or the in-memory offline store for a durable one.

## Design docs

See [`docs/`](docs/):

- [Development plan](docs/NG-MQTT-Client-Development-Plan.md)
- [Competitive research](docs/Competitive-Research-MQTT-Clients.md)
- [Phase 4 — Resilience design](docs/Phase-04-Resilience-Detailed-Design.md)

## License

MIT — see [LICENSE](LICENSE).
