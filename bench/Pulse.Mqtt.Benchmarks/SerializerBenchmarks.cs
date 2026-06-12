using System.Buffers;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Encode and decode throughput for the smallest useful PUBLISH, 10,000 packets per operation.
/// </summary>
[MemoryDiagnoser]
public class SerializerBenchmarks
{
    private readonly ArrayBufferWriter<byte> _output = new(4096);
    private readonly MqttPublishPacket _packet = new() { Topic = "A" };
    private byte[] _encoded = [];

    [GlobalSetup]
    public void Setup()
    {
        var scratch = new ArrayBufferWriter<byte>(256);
        MqttPacketWriter.Write(scratch, _packet);
        _encoded = scratch.WrittenSpan.ToArray();
    }

    [Benchmark]
    public void Serialize_10000_Messages()
    {
        for (var i = 0; i < 10_000; i++)
        {
            _output.ResetWrittenCount();
            MqttPacketWriter.Write(_output, _packet);
        }
    }

    [Benchmark]
    public void Deserialize_10000_Messages()
    {
        for (var i = 0; i < 10_000; i++)
        {
            MqttFrameReader.TryReadFrame(_encoded, out var header, out var body, out _);
            MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
        }
    }
}
