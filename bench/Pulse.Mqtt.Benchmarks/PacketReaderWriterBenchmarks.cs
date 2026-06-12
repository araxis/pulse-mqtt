using System.Buffers;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Codec;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Field-level buffer reader and writer throughput: a representative mix of strings, binary
/// blocks, single bytes, and variable-byte integers, 100,000 rounds per operation.
/// </summary>
[MemoryDiagnoser]
public class PacketReaderWriterBenchmarks
{
    private const string ShortString = "A relative short string.";
    private const string LongString =
        "fjgffiogfhgfhoihgoireghreghreguhreguireoghreouighreouighreughreguiorehreuiohruiorehreuioghreug";

    private readonly byte[] _demoPayload = new byte[1024];
    private readonly ArrayBufferWriter<byte> _output = new(8192);
    private byte[] _readPayload = [];

    [GlobalSetup]
    public void Setup()
    {
        var scratch = new ArrayBufferWriter<byte>(8192);
        WriteFields(new MqttBufferWriter(scratch));
        _readPayload = scratch.WrittenSpan.ToArray();
    }

    [Benchmark]
    public void Read_100_000_Messages()
    {
        for (var i = 0; i < 100_000; i++)
        {
            var reader = new MqttBufferReader(_readPayload);
            reader.ReadString();
            reader.ReadByte();
            reader.ReadByte();
            reader.ReadVarInt();
            reader.ReadString();
            reader.ReadVarInt();
            reader.ReadBinary();
            reader.ReadByte();
            reader.ReadByte();
            reader.ReadString();
            reader.ReadBinary();
        }
    }

    [Benchmark]
    public void Write_100_000_Messages()
    {
        for (var i = 0; i < 100_000; i++)
        {
            _output.ResetWrittenCount();
            WriteFields(new MqttBufferWriter(_output));
        }
    }

    private void WriteFields(MqttBufferWriter writer)
    {
        writer.WriteString(ShortString);
        writer.WriteByte(0x01);
        writer.WriteByte(0x02);
        writer.WriteVarInt(5647382);
        writer.WriteString(ShortString);
        writer.WriteVarInt(8574589);
        writer.WriteBinary(_demoPayload);
        writer.WriteByte(2);
        writer.WriteByte(0x02);
        writer.WriteString(LongString);
        writer.WriteBinary(_demoPayload);
    }
}
