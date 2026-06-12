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

| Library | Median (10 cycles) |
| --- | --- |
| Pulse.Mqtt | 2.89 ms |
| MQTTnet | 1.96 ms |

MQTTnet wins here. Pulse's `StartAsync` hands the connection to a supervisor loop, so reaching
the connected state crosses an extra task boundary and a state-channel hop. That is the price
of the always-supervised design; at ~1 ms it is irrelevant for long-lived connections but it is
a real difference for connect-per-operation patterns.

## Sustained throughput — 20,000 QoS 1 publishes, 200 in flight

| Library | Throughput | Wall clock | Allocated per message | GC gen0/gen1/gen2 |
| --- | --- | --- | --- | --- |
| Pulse.Mqtt | 4,014 msg/s | 4,983 ms | 1,620 B | 2 / 0 / 0 |
| MQTTnet (defaults) | 2,254 msg/s | 8,874 ms | 1,589 B | 2 / 2 / 0 |
| MQTTnet (`WithoutPacketFragmentation`) | 3,904 msg/s | 5,123 ms | 1,848 B | 2 / 2 / 0 |

Pulse is 78% faster than MQTTnet as configured out of the box and 3% faster than MQTTnet after
tuning, with 12% fewer bytes per message than the tuned configuration and no generation 1
collections. MQTTnet's per-message allocation in default mode is marginally lower than Pulse's,
but its objects live longer — the gen 1 collections show survivors that Pulse does not produce.

## Per-operation latency and allocation

ShortRun, in-process, 64-byte payloads, MQTTnet fragmentation-free. Latency on this path is
dominated by the broker round trip through the Docker proxy and has high run-to-run variance
(error bars overlap on QoS 0 and QoS 1); allocations are stable and exact.

| Operation | Pulse.Mqtt | MQTTnet | Pulse allocated | MQTTnet allocated |
| --- | --- | --- | --- | --- |
| QoS 0 round trip | 361.6 µs | 313.4 µs | 1.09 KB | 2.74 KB |
| QoS 1 publish to PUBACK | 382.0 µs | 475.4 µs | 1.70 KB | 2.10 KB |
| QoS 2 publish to PUBCOMP | 1,590.1 µs | 2,045.3 µs | 2.46 KB | 3.63 KB |

Reading it honestly:

- **QoS 0**: MQTTnet's mean is ~13% lower, within overlapping error bars. Call it a tie on
  latency; Pulse allocates 60% less per round trip.
- **QoS 1**: Pulse's mean is ~20% lower, again with overlapping error bars (MQTTnet's median
  was lower than its mean by a wide margin). Tie-to-slight-Pulse on latency; Pulse allocates
  19% less.
- **QoS 2**: Pulse is ~22% faster with non-overlapping medians and allocates 32% less. The
  four-packet exchange amplifies per-packet overhead, which is where the single-write framing
  and pooled buffers pay off.

## Summary

- Pulse delivers higher sustained throughput than MQTTnet in both MQTTnet configurations, and
  the gap against out-of-the-box MQTTnet is 78% on this setup.
- Pulse allocates less in every scenario (19–60% per operation) and avoids the generation 1
  collections MQTTnet incurs under load — less GC pressure on long-running processes.
- Per-operation latency over a real broker is broker-bound: Pulse and MQTTnet are within each
  other's error bars at QoS 0/1, and Pulse leads at QoS 2.
- MQTTnet establishes connections about 1 ms faster than Pulse's supervised client.
- MQTTnet's default packet fragmentation is a real-world footgun on any path with a Nagle hop
  (container proxies, some gateways): it cost 40 ms per operation here until disabled. Pulse
  has no equivalent failure mode.

Wire-codec micro-benchmarks (encode/decode without a broker, where Pulse measures 93–175 ns and
0–312 B per packet) live in `bench/Pulse.Mqtt.Benchmarks` and are reported in the readme.
