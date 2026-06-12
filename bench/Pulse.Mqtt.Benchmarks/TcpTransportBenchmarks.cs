using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Raw transport throughput: 10,000 five-byte chunks pushed by a peer and drained through the
/// transport's pipe input, over real loopback TCP and over the in-memory loopback pair.
/// </summary>
[MemoryDiagnoser]
public class TcpTransportBenchmarks
{
    private const int ChunkSize = 5;
    private const int Iterations = 10_000;

    private TcpListener _listener = null!;
    private NetworkStream _serverStream = null!;
    private IMqttTransport _tcpClient = null!;
    private IMqttTransport _loopbackClient = null!;
    private IMqttTransport _loopbackServer = null!;

    [GlobalSetup]
    public void Setup()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start(1);
        var accept = _listener.AcceptSocketAsync();

        var factory = new TcpTransportFactory(new TcpTransportOptions
        {
            Host = "127.0.0.1",
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port,
        });
        _tcpClient = factory.ConnectAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();

        var serverSocket = accept.GetAwaiter().GetResult();
        serverSocket.NoDelay = true;
        _serverStream = new NetworkStream(serverSocket, ownsSocket: true);

        (_loopbackClient, _loopbackServer) = LoopbackTransport.CreatePair();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _tcpClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _serverStream.Dispose();
        _listener.Stop();
        _loopbackClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _loopbackServer.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark]
    public Task Tcp_Send_10000_Chunks() =>
        Task.WhenAll(
            WriteTcpAsync(),
            DrainAsync(_tcpClient, Iterations * ChunkSize));

    [Benchmark]
    public Task Loopback_Send_10000_Chunks() =>
        Task.WhenAll(
            WriteLoopbackAsync(),
            DrainAsync(_loopbackClient, Iterations * ChunkSize));

    private async Task WriteTcpAsync()
    {
        await Task.Yield();
        var chunk = new byte[ChunkSize];
        for (var i = 0; i < Iterations; i++)
        {
            await _serverStream.WriteAsync(chunk);
        }
    }

    private async Task WriteLoopbackAsync()
    {
        await Task.Yield();
        var chunk = new byte[ChunkSize];
        for (var i = 0; i < Iterations; i++)
        {
            await _loopbackServer.Output.WriteAsync(chunk);
        }
    }

    private static async Task DrainAsync(IMqttTransport transport, long expected)
    {
        await Task.Yield();
        long read = 0;

        while (read < expected)
        {
            var result = await transport.Input.ReadAsync();
            read += result.Buffer.Length;
            transport.Input.AdvanceTo(result.Buffer.End);
        }
    }
}
