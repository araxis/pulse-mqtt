# Pulse.Mqtt vs MQTTnet

Head-to-head measurements of Pulse.Mqtt and MQTTnet 5.1.0 against a real Mosquitto broker.
The harness lives in `bench/Pulse.Mqtt.ComparisonBenchmarks` and reproduces every number below
with `dotnet run -c Release --project bench/Pulse.Mqtt.ComparisonBenchmarks` (requires Docker).

## Environment

| Item | Value |
| --- | --- |
| CPU | Intel Core i7-12700H (12th gen), 32 GB RAM |
| OS | Windows 11 (10.0.26200) |
| Runtime | .NET 10.0.9, X64 RyuJIT AVX2, workstation GC |
| Broker | eclipse-mosquitto:2 in Docker Desktop (WSL2), loopback TCP via the Docker port proxy |
| Pulse.Mqtt | this repository, Release build |
| MQTTnet | 5.1.0.1559 from NuGet |
| Benchmark tooling | BenchmarkDotNet 0.14.0, ShortRun, in-process toolchain, MemoryDiagnoser |

## Methodology

Both libraries use their user-facing clients (`ResilientMqttClient` vs `MqttClient`), the same
broker instance, the same 64-byte payloads, and the same topics per scenario. Three passes:

1. **Connect latency** — ten connect/disconnect cycles per library, wall-clock from client
   construction to the connected state, median reported.
2. **Sustained throughput** — 20,000 QoS 1 publishes with 200 concurrent in-flight operations.
   Wall clock, `GC.GetTotalAllocatedBytes` delta per message, and GC collection counts measured
   around the loop.
3. **Per-operation latency and allocation** — BenchmarkDotNet over a QoS 0 self-subscribed
   round trip (publish until the message arrives back) and QoS 1/QoS 2 publishes awaited to
   their acknowledgement.

Fairness rules baked into the harness:

- Every measured Pulse publish asserts the `Delivered` disposition, so a dropped connection
  can never let the offline queue fake fast numbers.
- MQTTnet runs with `WithoutPacketFragmentation()` in the per-operation and tuned-throughput
  runs. MQTTnet's default mode writes each packet as two TCP segments, which costs a ~40 ms
  delayed-acknowledgement stall per packet on the proxied loopback path; the default-mode
  result is reported separately below. Pulse always writes one buffer per packet and needs no
  tuning. Both libraries run with `NoDelay` sockets (each one's default).
- Clients connect once per process with unique client ids; reconnecting per benchmark case
  makes the broker revoke the previous session and poisons later cases.

## Connect latency

| Library | Median (30 cycles) |
| --- | --- |
| Pulse.Mqtt | 1.83 ms |
| MQTTnet | 1.93 ms |

An earlier revision lost this by about a millisecond: `StartAsync` queued the whole supervisor
through the thread pool, so even the TCP connect waited for a scheduler slot. The supervisor now
starts inline and reaches the socket connect on the calling task, which removed the gap while
keeping `StartAsync` non-blocking.

## Sustained throughput — 20,000 QoS 1 publishes, 200 in flight

| Library | Throughput | Wall clock | Allocated per message | GC gen0/gen1/gen2 |
| --- | --- | --- | --- | --- |
| Pulse.Mqtt | 4,009 msg/s | 4,989 ms | 1,494 B | 2 / 0 / 0 |
| MQTTnet (defaults) | 2,239 msg/s | 8,932 ms | 1,590 B | 2 / 2 / 0 |
| MQTTnet (`WithoutPacketFragmentation`) | 3,772 msg/s | 5,302 ms | 1,763 B | 2 / 2 / 0 |

Pulse is 79% faster than MQTTnet as configured out of the box and 6% faster than MQTTnet after
tuning, with the lowest bytes per message of the three configurations and no generation 1
collections — MQTTnet's gen 1 collections show survivors under load that Pulse does not produce.

## Per-operation latency and allocation

Fifteen iterations in-process, 64-byte payloads, MQTTnet fragmentation-free. Latency on this
path is dominated by the broker round trip through the Docker proxy, so medians are the robust
statistic; allocations are stable and exact.

| Operation | Pulse.Mqtt median | MQTTnet median | Pulse allocated | MQTTnet allocated |
| --- | --- | --- | --- | --- |
| QoS 0 round trip | 408 µs | 529 µs | 892 B | 2,802 B |
| QoS 1 publish to PUBACK | 471 µs | 525 µs | 1,547 B | 2,156 B |
| QoS 2 publish to PUBCOMP | 924 µs | 909 µs | 2,161 B | 3,717 B |

Reading it honestly:

- **QoS 0**: Pulse's median is 23% lower and it allocates 68% less per round trip. The earlier
  revision trailed here; completing acknowledgements on the receive loop and delivering
  messages without an intermediate forwarding queue removed two task wake-ups per message.
- **QoS 1**: Pulse's median is 10% lower with 28% less allocation.
- **QoS 2**: medians are within 1.6% of each other — inside the noise (the standard error is
  several times that) — with Pulse ahead on minimum, first quartile, and allocating 42% less.
  The four-packet exchange is two full broker round trips, so the wire dominates both clients
  equally.

## Summary

- Pulse delivers higher sustained throughput than MQTTnet in both MQTTnet configurations: 79%
  ahead of out-of-the-box MQTTnet, 6% ahead of the tuned configuration, with the lowest
  per-message allocation of the three.
- Pulse connects faster (1.83 ms vs 1.93 ms median over 30 cycles).
- Pulse's per-operation medians lead at QoS 0 (−23%) and QoS 1 (−10%) and are statistically
  tied at QoS 2.
- Pulse allocates 28–68% less in every per-operation scenario and produces no generation 1
  collections under load, where MQTTnet does — less GC pressure on long-running processes.
- MQTTnet's default packet fragmentation is a real-world footgun on any path with a Nagle hop
  (container proxies, some gateways): it cost 40 ms per operation here until disabled. Pulse
  has no equivalent failure mode.

Wire-codec micro-benchmarks (encode/decode without a broker: 60 ns and zero allocation per
PUBLISH encode, 96 ns per decode) live in `bench/Pulse.Mqtt.Benchmarks` and are documented in
[Benchmark-Suite.md](Benchmark-Suite.md).
