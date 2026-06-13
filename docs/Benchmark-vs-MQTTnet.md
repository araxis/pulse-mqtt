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
| Benchmark tooling | BenchmarkDotNet 0.15.8, 15 iterations, in-process toolchain, MemoryDiagnoser |

::: warning Numbers move with the environment
These tables were re-baselined after the container tooling update (Testcontainers 4.12)
changed the Docker networking path — latencies shifted for **both** libraries relative to
earlier revisions of this page. Compare numbers within one table, not across page revisions;
allocation numbers are exact and stable, wall-clock numbers carry the proxied-loopback noise.
:::

::: tip Release-candidate validation
The benchmark suites were re-run on the 1.0.0-rc.1 build. The hot path — the wire codec, the
publish/subscribe path, and the connection layer — is unchanged since these numbers were
published, so the allocation figures (which are exact) and per-operation latencies remain
representative; the only `Pulse.Mqtt.Core` change in the release candidate is disposal-teardown
hardening, off the measured hot path. Reproduce with the command above (comparison, requires
Docker) or `dotnet run -c Release --project bench/Pulse.Mqtt.Benchmarks` (the allocation
micro-benchmarks, no Docker).
:::

## Methodology

Both libraries use their user-facing clients (`ResilientMqttClient` vs `MqttClient`), the same
broker instance, the same 64-byte payloads, and the same topics per scenario. The broker runs
with `max_inflight_messages 0` (unlimited) so the throughput pass measures the clients rather
than Mosquitto's default 20-message receive maximum — relevant because **Pulse honors the
broker's receive maximum and MQTTnet does not** (see the compliance note below). Three passes:

1. **Connect latency** — thirty connect/disconnect cycles per library, wall-clock from client
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
| Pulse.Mqtt | 2.29 ms |
| MQTTnet | 1.93 ms |

MQTTnet connects faster on this path by roughly a third of a millisecond. Pulse's supervised design
crosses one more task boundary on the way to the connected state; for long-lived connections
this is irrelevant, for connect-per-operation patterns it is a real difference. (An earlier
supervisor fix already removed a full thread-pool hop here; the remaining gap is the price of
the always-supervised architecture.)

## Sustained throughput — 20,000 QoS 1 publishes, 200 in flight

| Library | Throughput | Wall clock | Allocated per message | GC gen0/gen1/gen2 |
| --- | --- | --- | --- | --- |
| Pulse.Mqtt | **3,862 msg/s** | 5,178 ms | 1,645 B | **2 / 0 / 0** |
| MQTTnet (defaults) | 2,767 msg/s | 7,228 ms | 1,665 B | 2 / 2 / 0 |
| MQTTnet (`WithoutPacketFragmentation`) | 3,261 msg/s | 6,133 ms | 1,749 B | 2 / 2 / 0 |

On this run Pulse leads throughput across both MQTTnet configurations and allocates marginally
less per message, while producing **no generation 1 collections** where MQTTnet incurs them.
Throughput ordering on this proxied-loopback path swings run to run (a prior revision of this page
had MQTTnet narrowly ahead), so read it as broadly comparable; the durable signals are the GC
profile and the compliance asymmetry below. Pulse's per-message allocation here predates the 1.1.0
optimization that removed a per-publish metric-tag string allocation, so a fresh run reads a little
lower again.

**The compliance asymmetry matters more than the percentages.** Pulse enforces the broker's
CONNACK receive maximum; MQTTnet does not. Against an out-of-the-box Mosquitto (which
advertises a receive maximum of 20), Pulse correctly holds 20 publishes in flight while
MQTTnet runs 200 — exceeding a broker's receive maximum is a protocol violation the broker may
answer with a disconnect. The table above intentionally removes the broker's cap to compare
raw client throughput on equal terms.

## Per-operation latency and allocation

Fifteen iterations in-process, 64-byte payloads, MQTTnet fragmentation-free. Latency on this
path is dominated by the broker round trip through the Docker proxy and carries wide error
bars; allocations are stable and exact.

| Operation | Pulse.Mqtt mean | MQTTnet mean | Pulse allocated | MQTTnet allocated |
| --- | --- | --- | --- | --- |
| QoS 0 round trip | 926 µs | 888 µs | **922 B** | 2,804 B |
| QoS 1 publish to PUBACK | **880 µs** | 951 µs | **1,701 B** | 2,156 B |
| QoS 2 publish to PUBCOMP | **1,479 µs** | 1,552 µs | **2,473 B** | 3,725 B |

Reading it honestly:

- **Allocations are the stable result**: Pulse allocates 21–67% less in every scenario, and
  that holds across every run and environment this comparison has been executed on. (The figures
  here predate the 1.1.0 metric-tag allocation fix, so a fresh run reads slightly lower again.)
- **Latency means overlap heavily** (standard deviations of 90–250 µs on means under 1.7 ms).
  On this run Pulse leads QoS 1 and QoS 2 and trails QoS 0. Treat per-operation latency as
  parity-within-noise on a proxied loopback, and benchmark on your own network for decisions that
  depend on it.
- **The QoS 2 numbers reflect MQTT 5 §4.9 compliance**: Pulse holds the receive-maximum send
  quota until PUBCOMP (the final acknowledgement), and still beats MQTTnet on both latency and
  allocation.

## Summary

- **Stable across every environment tested**: Pulse allocates marginally less per message under
  sustained load and 21–67% less per operation, with zero generation 1 collections where
  MQTTnet incurs them — materially less GC pressure on long-running processes. (The 1.1.0
  metric-tag allocation fix widens the per-operation lead further.)
- **Throughput leads or ties** depending on the run; against an out-of-the-box Mosquitto, Pulse
  is the only one of the two that honors the broker's receive maximum (MQTTnet exceeds it, which
  is a protocol violation), and Pulse holds the QoS 2 send quota to PUBCOMP per MQTT 5 §4.9.
- **Latency is broker-path-bound** with overlapping error bars; leads trade places between
  runs and Docker networking stacks. MQTTnet held connect latency and QoS 0 on this baseline;
  Pulse held QoS 1 and QoS 2.
- MQTTnet's default packet fragmentation cost ~40 ms per operation on the previous Docker
  networking stack until disabled; the current stack masks it. Pulse writes one buffer per
  packet and has no configuration-dependent failure mode either way.

Wire-codec micro-benchmarks (encode/decode without a broker: 60 ns and zero allocation per
PUBLISH encode, 96 ns per decode) live in `bench/Pulse.Mqtt.Benchmarks` and are documented in
[Benchmark-Suite.md](Benchmark-Suite.md).
