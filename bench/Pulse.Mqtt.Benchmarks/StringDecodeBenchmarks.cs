using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Codec;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Decoding a long UTF-8 string field: the buffer reader's length-prefixed read against the raw
/// encoding call it builds on.
/// </summary>
[MemoryDiagnoser]
public class StringDecodeBenchmarks
{
    private const string LongString =
        "hgfjkdfkjlghfdjghdfljkdfhgdlkjfshgsldkfjghsdflkjghdsflkjhrstiuoghlkfjbhnfbutghjoiöjhklötnbhtroliöuhbjntluiobkjzbhtdrlskbhtruhjkfthgbkftgjhgfiklhotriuöhbjtrsioöbtrsötrhträhtrühjtriüoätrhjtsrölbktrbnhtrulöbionhströloubinströoliubhnsöotrunbtöroisntröointröioujhgötiohjgötorshjnbgtöorihbnjtröoihbjntröobntröoibntrjhötrohjbtröoihntröoibnrtoiöbtrjnboöitrhjtnriohötrhjtöroihjtroöihjtroösibntsroönbotöirsbntöoihjntröoihntroöbtrboöitrnhoöitrhjntröoishbnjtröosbhtröbntriohjtröoijtöoitbjöotibjnhöotirhbjntroöibhnjrtoöibnhtroöibnhtörsbnhtöoirbnhtöroibntoörhjnbträöbtrbträbtrbtirbätrsibohjntrsöiobthnjiohjsrtoib";

    private byte[] _lengthPrefixed = [];
    private byte[] _rawUtf8 = [];

    [GlobalSetup]
    public void Setup()
    {
        var scratch = new ArrayBufferWriter<byte>(4096);
        new MqttBufferWriter(scratch).WriteString(LongString);
        _lengthPrefixed = scratch.WrittenSpan.ToArray();
        _rawUtf8 = Encoding.UTF8.GetBytes(LongString);
    }

    [Benchmark]
    public string Pulse_ReadString()
    {
        var reader = new MqttBufferReader(_lengthPrefixed);
        return reader.ReadString();
    }

    [Benchmark(Baseline = true)]
    public string Utf8_GetString() => Encoding.UTF8.GetString(_rawUtf8);
}
