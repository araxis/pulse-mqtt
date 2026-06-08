using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Codec;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Measures the variable-length integer codec on the hot path. Expectation: zero managed
/// allocation for both encode and decode (verified via <see cref="MemoryDiagnoserAttribute"/>).
/// </summary>
[MemoryDiagnoser]
public class VarIntBenchmarks
{
    private static readonly uint[] Values =
        [0, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, 268_435_455];

    private readonly byte[] _scratch = new byte[MqttVarInt.MaxEncodedLength];

    [Benchmark]
    public int EncodeAll()
    {
        var total = 0;
        foreach (var value in Values)
        {
            total += MqttVarInt.Write(_scratch, value);
        }

        return total;
    }

    [Benchmark]
    public uint RoundTripAll()
    {
        uint sum = 0;
        foreach (var value in Values)
        {
            MqttVarInt.Write(_scratch, value);
            MqttVarInt.TryRead(_scratch, out var read, out _);
            sum += read;
        }

        return sum;
    }
}
