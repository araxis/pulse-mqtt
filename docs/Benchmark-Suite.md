# Benchmark suite

`bench/Pulse.Mqtt.Benchmarks` covers the same ground as the upstream `MQTTnet.Benchmarks`
project, scenario for scenario, so the two libraries can be profiled along the same axes. Every
class carries `MemoryDiagnoser`; run any subset with:

```
dotnet run -c Release --project bench/Pulse.Mqtt.Benchmarks -- --filter <pattern> --job short
```

For the direct head-to-head numbers against MQTTnet over a real broker, see
[Benchmark-vs-MQTTnet.md](Benchmark-vs-MQTTnet.md).

## Scenario map

| MQTTnet benchmark | Pulse counterpart | What it measures |
| --- | --- | --- |
| `SerializerBenchmark` | `SerializerBenchmarks` | Encode and decode 10,000 minimal PUBLISH packets |
| `MqttPacketReaderWriterBenchmark` | `PacketReaderWriterBenchmarks` | 100,000 rounds of mixed field reads/writes |
| `MqttBufferReaderBenchmark` | `StringDecodeBenchmarks` | Long UTF-8 string decode against the raw encoding call |
| `TopicFilterComparerBenchmark` | `TopicFilterComparerBenchmarks` | 100,000 rounds of eight representative filter matches |
| `MemoryCopyBenchmark` | `MemoryCopyBenchmarks` | Array copy against span copy at codec block sizes |
| `AsyncLockBenchmark` | `SendLockBenchmarks` | The send-path lock: contended hand-off and raw acquire/release |
| `LoggerBenchmark` | `LoggerBenchmarks` | 10,000 log calls through source-generated `LoggerMessage` |
| `ChannelAdapterBenchmark` | `ConnectionBenchmarks` | 10,000 packets through the packet engine, both directions |
| `SendPacketAsyncBenchmark` | `PacketPipeBenchmarks.Send_Small_Packet` | One small packet encoded and flushed to a `PipeWriter` |
| `ReaderExtensionsBenchmark` | `PacketPipeBenchmarks.Decode_Large_Publish` | One 10 KB PUBLISH framed and decoded from a `PipeReader` |
| `MqttTcpChannelBenchmark` | `TcpTransportBenchmarks.Tcp_Send_10000_Chunks` | 10,000 five-byte chunks over loopback TCP |
| `TcpPipesBenchmark` | `TcpTransportBenchmarks.Loopback_Send_10000_Chunks` | The same chunks over the in-memory transport pair |
| `MessageProcessingBenchmark` | `MessageProcessingBenchmarks` | 10,000 awaited QoS 0 publishes through the client to a broker |
| `MessageDeliveryBenchmark` | `MessageDeliveryBenchmarks` | Fan-out delivery to ten subscribers across a large topic space |
| `SubscribeBenchmark` | `SubscribeBenchmarks` | 10,000 awaited single-topic SUBSCRIBE round trips |
| `UnsubscribeBenchmark` | `UnsubscribeBenchmarks` | 10,000 awaited single-topic UNSUBSCRIBE round trips |
| `VarIntBenchmarks`, `PacketBenchmarks` | (Pulse originals) | Variable-byte integers; single-packet encode/decode and routing |

Not ported: `MessageProcessingMqttConnectionContextBenchmark` (ASP.NET Core server transport —
Pulse has no server), and `RoundtripProcessingBenchmark` / `ServerProcessingBenchmark`, whose
bodies are commented out upstream and measure nothing.

Where MQTTnet's broker-based scenarios start `MqttServer`, the Pulse counterparts run against
`PulseMqttTestBroker` over the in-memory loopback transport — the equivalent in-process broker
this library ships for testing. The TCP scenarios use a real socket pair.

## Results

ShortRun job (3 iterations), Intel Core i7-12700H, Windows 11, .NET 10.0.9, workstation GC.
ShortRun error bars are wide; treat means as indicative, allocations as exact.

### Codec and primitives

| Benchmark | Mean | Allocated |
| --- | --- | --- |
| Serialize_10000_Messages | 1.77 ms (177 ns each) | 64 B per packet |
| Deserialize_10000_Messages | 0.94 ms (94 ns each) | 208 B per packet |
| Read_100_000_Messages (11 fields each) | 12.7 ms | 360 B per round (the three decoded strings) |
| Write_100_000_Messages (11 fields each) | 18.8 ms | 0 B |
| Pulse_ReadString (long string) | 763 ns | parity with `Encoding.UTF8.GetString` (0.97 ratio) |
| Match_100_000_Rounds (8 filters each) | 24.4 ms (~30 ns per match) | 0 B |
| Send_Small_Packet (pipe) | 405 ns | 416 B |
| Decode_Large_Publish (10 KB, pipe) | 2.71 µs | 11.1 KB (the payload copy) |

### Engine and locking

| Benchmark | Mean | Allocated |
| --- | --- | --- |
| Connection Send_10000_Messages | 3.00 ms (300 ns each) | 64 B per packet |
| Connection Receive_10000_Messages | 3.37 ms (337 ns each) | 209 B per packet |
| SendLock Wait_100_000_Times | 4.17 ms (42 ns per acquire/release) | 136 B total |
| SendLock Synchronize_100_Tasks | 1.55 s (dominated by the deliberate 5 ms hold) | 53 KB |
| Log_10000_Messages_NullLogger | 3.1 µs (0.3 ns each) | 0 B |
| Log_10000_Messages_Disabled_Level | 6.7 µs (0.7 ns each) | 0 B |
| Log_10000_Messages_Enabled_Sink | 731 µs | 104 B per call (the rendered string) |

### Client, broker, and transport

| Benchmark | Mean | Allocated |
| --- | --- | --- |
| Send_10000_Messages (client to broker, QoS 0) | 8.48 ms (848 ns each) | 430 B per publish, both sides |
| DeliverMessages (10 subscribers × 5 topics) | 239 µs | 68 KB |
| DeliverMessages (10 subscribers × 50 topics) | 8.03 ms | 716 KB |
| Subscribe_10000_Topics | 1.92 s | 1.51 GB |
| Unsubscribe_10000_Topics | 161 ms | 19.4 MB |
| Tcp_Send_10000_Chunks (5 B each, loopback TCP) | 149 ms | 0 B |
| Loopback_Send_10000_Chunks (in-memory pair) | 1.17 ms | 466 B |
| MemoryCopy Array vs Span (63–5095 B) | parity within noise | 0 B |

Two of these flagged real costs rather than measuring healthy paths. Subscribing topics one at a
time snapshots the durable subscription set per call — quadratic time and allocation at high
subscription counts. The TCP chunk drain trails the in-memory pair by two orders of magnitude,
pointing at per-chunk overhead in the socket-to-pipe path. Both are tracked for optimization;
the tables above record the suite as first measured.
