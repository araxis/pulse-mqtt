using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Transport;

public sealed class TcpTransportTests
{
    [Fact]
    public async Task Connects_and_exchanges_bytes_with_a_local_listener()
    {
        using var listener = new Socket(SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(1);
        var port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        var factory = new TcpTransportFactory(new TcpTransportOptions { Host = "127.0.0.1", Port = port });

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var acceptTask = listener.AcceptAsync(timeout.Token).AsTask();
        await using var transport = await factory.ConnectAsync(timeout.Token);
        using var serverSocket = await acceptTask;

        // client -> server
        var hello = "ping"u8.ToArray();
        await transport.Output.WriteAsync(hello, timeout.Token);
        var received = new byte[hello.Length];
        var read = await serverSocket.ReceiveAsync(received, timeout.Token);
        read.ShouldBe(hello.Length);
        received.ShouldBe(hello);

        // server -> client
        var reply = "pong"u8.ToArray();
        await serverSocket.SendAsync(reply, timeout.Token);
        var result = await transport.Input.ReadAtLeastAsync(reply.Length, timeout.Token);
        result.Buffer.ToArray()[..reply.Length].ShouldBe(reply);
        transport.Input.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task Connect_honors_cancellation()
    {
        // A TEST-NET address that will not answer; cancellation must abort the attempt promptly.
        var factory = new TcpTransportFactory(new TcpTransportOptions { Host = "192.0.2.1", Port = 1883 });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await factory.ConnectAsync(cts.Token));
    }
}
