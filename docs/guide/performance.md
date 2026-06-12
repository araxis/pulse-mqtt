# Performance

Performance claims here are measured, repeatable, and published — including the scenarios
where the competition wins a metric. Hardware for all numbers below: Intel i7-12700H,
Windows 11, .NET 10, workstation GC.

## Head to head with MQTTnet

Both libraries driving their user-facing clients against the same real Mosquitto broker,
identical payloads, topics, and QoS. Full methodology and tables:
[the MQTTnet comparison](/Benchmark-vs-MQTTnet).

| Metric | Pulse.Mqtt | MQTTnet 5.1 |
| --- | --- | --- |
| Sustained throughput (20k QoS 1, 200 in flight) | **4,009 msg/s** | 2,239 default / 3,772 tuned |
| Allocated per message under that load | **1,494 B** | 1,590 / 1,763 B |
| GC under that load (gen0/1/2) | **2 / 0 / 0** | 2 / 2 / 0 |
| Connect latency (median of 30) | **1.83 ms** | 1.93 ms |
| QoS 0 round trip (median) | **408 µs** | 529 µs |
| QoS 1 publish→PUBACK (median) | **471 µs** | 525 µs |
| QoS 2 publish→PUBCOMP (median) | 924 µs (tie within noise) | 909 µs |
| Allocation per operation | **28–68% less** in every scenario | — |

Read honestly: QoS 2 medians are statistically indistinguishable — that exchange is two full
broker round trips, which bound both clients equally; Pulse leads its minimum, first quartile,
and allocates 42% less.

## Micro-benchmarks

From the [benchmark suite](/Benchmark-Suite), which mirrors the upstream MQTTnet benchmark
project scenario for scenario:

| Operation | Mean | Allocated |
| --- | --- | --- |
| PUBLISH encode (v5, no properties) | ~60 ns | **0 B** |
| Frame + decode the same packet | ~96 ns | 144 B (the decoded object) |
| Topic filter match | ~32 ns | 0 B |
| Route template match (2 captures) | ~56 ns | 104 B (the captured values) |
| Variable-length integer round trip | ~26 ns | 0 B |
| 10,000 awaited publishes through the in-process broker | 865 ns each | 350 B (both sides) |
| Subscribe 10,000 topics, each acknowledged | 119 ms | 19 MB |

## Where the numbers come from

- **Single-pass encoding.** A publish without v5 properties is sized up front and written
  straight into the transport buffer — zero intermediate allocation, the payload copied once.
- **Single-write framing.** One packet, one TCP write: no fragmentation, no Nagle stalls on
  proxied paths (a default-configuration MQTTnet publish lost ~40 ms per operation to exactly
  that in our measurements).
- **Receive-loop dispatch.** Acknowledgements complete their waiters directly on the receive
  loop; inbound messages take one bounded queue to your handler, not three.
- **Bounded queues everywhere.** Backpressure flows to the socket; the GC table above —
  zero gen 1 collections under sustained load — is the visible result.
- **Pooled buffers** for everything that cannot be encoded in place.

## Reproduce everything

```shell
# Micro-benchmarks (no infrastructure):
dotnet run -c Release --project bench/Pulse.Mqtt.Benchmarks -- --filter * --job short

# Head-to-head (requires Docker for Mosquitto):
dotnet run -c Release --project bench/Pulse.Mqtt.ComparisonBenchmarks
```
