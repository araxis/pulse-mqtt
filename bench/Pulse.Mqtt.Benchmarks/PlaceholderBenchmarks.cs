using BenchmarkDotNet.Attributes;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Harness smoke benchmark. Real codec benchmarks (zero-alloc publish-encode,
/// decode throughput) arrive with Phase 1.
/// </summary>
[MemoryDiagnoser]
public class PlaceholderBenchmarks
{
    [Benchmark]
    public int Baseline() => 1 + 1;
}
