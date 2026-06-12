using BenchmarkDotNet.Attributes;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Buffer copy strategies at the block sizes the codec moves: classic array copy against the
/// span copy the decode path uses when reassembling fragmented bodies.
/// </summary>
[MemoryDiagnoser]
public class MemoryCopyBenchmarks
{
    private const int MaxLength = 1024 * 8;

    private readonly byte[] _source = new byte[MaxLength];
    private readonly byte[] _target = new byte[MaxLength];

    [Params(64 - 1, 128 - 1, 256 - 1, 512 - 1, 1024 - 1, 2048 - 1, 5096 - 1)]
    public int Length { get; set; }

    [Benchmark(Baseline = true)]
    public void Array_Copy() => Array.Copy(_source, 0, _target, 0, Length);

    [Benchmark]
    public void Span_Copy() => _source.AsSpan(0, Length).CopyTo(_target);
}
