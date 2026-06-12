using System.Buffers;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// The packet engine end to end: sending 10,000 PUBLISH packets through
/// <see cref="MqttConnection"/> into a null sink, and receiving 10,000 from a preloaded pipe
/// through framing, decode, and the inbound queue.
/// </summary>
[MemoryDiagnoser]
public class ConnectionBenchmarks
{
    private readonly MqttPublishPacket _packet = new() { Topic = "A" };
    private MqttConnection _sendConnection = null!;
    private byte[] _receiveBlob = [];

    [GlobalSetup]
    public void Setup()
    {
        _sendConnection = new MqttConnection(new NullSinkTransport(), new MqttConnectionOptions());

        var scratch = new ArrayBufferWriter<byte>(256);
        MqttPacketWriter.Write(scratch, _packet);
        var encoded = scratch.WrittenSpan.ToArray();

        _receiveBlob = new byte[encoded.Length * 10_000];
        for (var i = 0; i < 10_000; i++)
        {
            encoded.CopyTo(_receiveBlob, i * encoded.Length);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _sendConnection.DisposeAsync().AsTask().GetAwaiter().GetResult();

    [Benchmark]
    public async Task Send_10000_Messages()
    {
        for (var i = 0; i < 10_000; i++)
        {
            await _sendConnection.SendAsync(_packet, CancellationToken.None);
        }
    }

    [Benchmark]
    public async Task<int> Receive_10000_Messages()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(_receiveBlob);
        await pipe.Writer.CompleteAsync();

        await using var connection = new MqttConnection(
            new ReplayTransport(pipe.Reader),
            new MqttConnectionOptions());
        connection.Start();

        var received = 0;
        await foreach (var _ in connection.Inbound.ReadAllAsync())
        {
            received++;
        }

        return received != 10_000
            ? throw new InvalidOperationException($"Received {received} of 10000.")
            : received;
    }

    private sealed class NullSinkTransport : IMqttTransport
    {
        public PipeReader Input { get; } = PipeReader.Create(Stream.Null);

        public PipeWriter Output { get; } = PipeWriter.Create(Stream.Null);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReplayTransport(PipeReader input) : IMqttTransport
    {
        public PipeReader Input { get; } = input;

        public PipeWriter Output { get; } = PipeWriter.Create(Stream.Null);

        public ValueTask DisposeAsync()
        {
            Input.Complete();
            return ValueTask.CompletedTask;
        }
    }
}
