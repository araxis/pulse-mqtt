using System.Buffers;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>Hot-path packet work: encode, frame + decode, and topic matching.</summary>
[MemoryDiagnoser]
public class PacketBenchmarks
{
    private readonly ArrayBufferWriter<byte> _output = new(512);
    private readonly MqttPublishPacket _publish = new()
    {
        Topic = "plant/line1/sensors/temp",
        Payload = "{\"value\":21.5,\"unit\":\"C\"}"u8.ToArray(),
        QualityOfService = MqttQualityOfService.AtLeastOnce,
        PacketIdentifier = 42,
    };

    private byte[] _encoded = [];
    private readonly MqttRouteTemplate _template = MqttRouteTemplate.Parse("plant/{line}/sensors/{kind}");

    [GlobalSetup]
    public void Setup()
    {
        var scratch = new ArrayBufferWriter<byte>(512);
        MqttPublishCodec.Encode(scratch, _publish);
        _encoded = scratch.WrittenSpan.ToArray();
    }

    [Benchmark]
    public int PublishEncode()
    {
        _output.Clear();
        MqttPublishCodec.Encode(_output, _publish);
        return _output.WrittenCount;
    }

    [Benchmark]
    public MqttPacket FrameAndDecode()
    {
        MqttFrameReader.TryReadFrame(_encoded, out var header, out var body, out _);
        return MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
    }

    [Benchmark]
    public bool FilterMatch() => MqttTopicFilterMatcher.Matches("plant/+/sensors/#", "plant/line1/sensors/temp");

    [Benchmark]
    public bool TemplateMatch()
    {
        return _template.TryMatch("plant/line1/sensors/temp", out _);
    }
}
