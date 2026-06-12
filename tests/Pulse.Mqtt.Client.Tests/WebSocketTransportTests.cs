using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class WebSocketTransportTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task Packets_round_trip_over_a_websocket()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var (listener, uri) = StartListener();
        using var ownedListener = listener;

        var acceptTask = AcceptWebSocketAsync(listener);
        var factory = new WebSocketTransportFactory(new WebSocketTransportOptions { Uri = uri });
        await using var transport = await factory.ConnectAsync(timeout.Token);
        using var server = await acceptTask.WaitAsync(timeout.Token);

        // client -> server: a PINGREQ
        MqttPingCodec.WriteRequest(transport.Output);
        await transport.Output.FlushAsync(timeout.Token);
        var received = await ReceiveFrameAsync(server, timeout.Token);
        received.ShouldBe(new byte[] { 0xC0, 0x00 });

        // server -> client: a CONNACK
        var scratch = new ArrayBufferWriter<byte>();
        MqttConnAckCodec.Encode(scratch, new MqttConnAckPacket { SessionPresent = true });
        await server.SendAsync(scratch.WrittenMemory, WebSocketMessageType.Binary, endOfMessage: true, timeout.Token);

        var result = await transport.Input.ReadAtLeastAsync(scratch.WrittenCount, timeout.Token);
        var bytes = result.Buffer.ToArray();
        var status = MqttFrameReader.TryReadFrame(bytes, out var header, out var body, out _);
        status.ShouldBe(MqttFrameStatus.Complete);
        MqttConnAckCodec.Decode(body, MqttProtocolVersion.V500).SessionPresent.ShouldBeTrue();
        transport.Input.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task The_raw_client_handshakes_over_a_websocket()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var (listener, uri) = StartListener();
        using var ownedListener = listener;

        // The server socket must stay open until the client has read the CONNACK; disposing it
        // inside the task can abort the connection before the bytes are consumed.
        var serverTask = Task.Run(async () =>
        {
            var server = await AcceptWebSocketAsync(listener);
            try
            {
                var connect = await ReceiveFrameAsync(server, timeout.Token);
                var status = MqttFrameReader.TryReadFrame(connect, out _, out var body, out _);
                status.ShouldBe(MqttFrameStatus.Complete);
                MqttConnectCodec.Decode(body).ClientId.ShouldBe("ws-client");

                var scratch = new ArrayBufferWriter<byte>();
                MqttConnAckCodec.Encode(scratch, new MqttConnAckPacket());
                await server.SendAsync(scratch.WrittenMemory, WebSocketMessageType.Binary, endOfMessage: true, timeout.Token);
            }
            catch
            {
                server.Dispose();
                throw;
            }

            return server;
        }, timeout.Token);

        var factory = new WebSocketTransportFactory(new WebSocketTransportOptions { Uri = uri });
        await using var client = new RawMqttClient(factory);
        var connAck = await client.ConnectAsync(
            new MqttConnectPacket { ClientId = "ws-client", KeepAliveSeconds = 0 }, timeout.Token);

        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        using var server = await serverTask;
    }

    [Fact]
    public void Non_websocket_schemes_are_rejected()
    {
        Should.Throw<ArgumentException>(() => new WebSocketTransportOptions { Uri = new Uri("http://broker") });
    }

    private static (HttpListener Listener, Uri Uri) StartListener()
    {
        var port = FreePort();
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        listener.Start();
        return (listener, new Uri($"ws://127.0.0.1:{port}/"));
    }

    private static int FreePort()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        return ((IPEndPoint)probe.LocalEndpoint).Port;
    }

    private static async Task<WebSocket> AcceptWebSocketAsync(HttpListener listener)
    {
        var context = await listener.GetContextAsync();
        var webSocketContext = await context.AcceptWebSocketAsync(subProtocol: "mqtt");
        return webSocketContext.WebSocket;
    }

    private static async Task<byte[]> ReceiveFrameAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        var received = await socket.ReceiveAsync(buffer, cancellationToken);
        return buffer[..received.Count];
    }
}
