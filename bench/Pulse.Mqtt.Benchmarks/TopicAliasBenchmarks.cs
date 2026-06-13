using System.Buffers;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// The wire saving from topic aliases on a repeated-topic workload: the same publish encoded
/// with its full topic on every message, against the alias form an established mapping allows.
/// </summary>
[MemoryDiagnoser]
public class TopicAliasBenchmarks
{
    private const string LongTopic = "plant/building3/line7/station12/sensors/temperature/celsius";

    private readonly ArrayBufferWriter<byte> _output = new(4096);
    private readonly MqttPublishPacket _plain = new() { Topic = LongTopic, Payload = new byte[8] };
    private readonly MqttPublishPacket _aliased = new() { Topic = string.Empty, TopicAlias = 1, Payload = new byte[8] };

    [Benchmark(Baseline = true)]
    public int Encode_1000_Publishes_Full_Topic()
    {
        var total = 0;
        for (var i = 0; i < 1000; i++)
        {
            _output.ResetWrittenCount();
            MqttPacketWriter.Write(_output, _plain);
            total += _output.WrittenCount;
        }

        return total;
    }

    [Benchmark]
    public int Encode_1000_Publishes_Aliased()
    {
        var total = 0;
        for (var i = 0; i < 1000; i++)
        {
            _output.ResetWrittenCount();
            MqttPacketWriter.Write(_output, _aliased);
            total += _output.WrittenCount;
        }

        return total;
    }
}
