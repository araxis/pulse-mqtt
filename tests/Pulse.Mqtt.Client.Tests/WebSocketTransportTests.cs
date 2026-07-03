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
    public async Task Bytes_flushed_just_before_disposal_reach_the_peer()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var (listener, uri) = StartListener();
        using var ownedListener = listener;

        var acceptTask = AcceptWebSocketAsync(listener);
        var factory = new WebSocketTransportFactory(new WebSocketTransportOptions { Uri = uri });
        var transport = await factory.ConnectAsync(timeout.Token);
        using var server = await acceptTask.WaitAsync(timeout.Token);

        // A graceful MQTT shutdown flushes the DISCONNECT (a PINGREQ stand-in here) and disposes
        // straight after. The buffered send pump must drain those bytes and run the close handshake
        // before disposal cancels it — otherwise the peer loses the bytes and sees an abrupt close.
        MqttPingCodec.WriteRequest(transport.Output);
        await transport.Output.FlushAsync(timeout.Token);
        await transport.DisposeAsync();

        var received = await ReceiveFrameAsync(server, timeout.Token);
        received.ShouldBe(new byte[] { 0xC0, 0x00 });
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

    [Fact]
    public async Task Custom_headers_are_sent_on_the_handshake()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var (listener, uri) = StartListener();
        using var ownedListener = listener;

        var headerSeen = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var acceptTask = Task.Run(async () =>
        {
            var context = await listener.GetContextAsync();
            headerSeen.TrySetResult(context.Request.Headers["Authorization"]);
            var webSocketContext = await context.AcceptWebSocketAsync(subProtocol: "mqtt");
            return webSocketContext.WebSocket;
        }, timeout.Token);

        var factory = new WebSocketTransportFactory(new WebSocketTransportOptions
        {
            Uri = uri,
            Headers = new Dictionary<string, string> { ["Authorization"] = "Bearer token-123" },
        });

        await using var transport = await factory.ConnectAsync(timeout.Token);
        using var server = await acceptTask.WaitAsync(timeout.Token);

        (await headerSeen.Task.WaitAsync(timeout.Token)).ShouldBe("Bearer token-123");
    }

    [Fact]
    public async Task An_unreachable_proxy_fails_the_connection()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var (listener, uri) = StartListener();
        using var ownedListener = listener;

        // Routing through a dead proxy must fail, which proves the proxy is actually applied.
        var factory = new WebSocketTransportFactory(new WebSocketTransportOptions
        {
            Uri = uri,
            Proxy = new WebProxy($"http://127.0.0.1:{FreePort()}"),
        });

        await Should.ThrowAsync<WebSocketException>(() => factory.ConnectAsync(timeout.Token).AsTask());
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

    [Fact]
    public async Task Disposal_stays_bounded_when_the_close_handshake_blocks()
    {
        var socket = new BlockingCloseWebSocket();
        var transport = new WebSocketTransport(socket);

        MqttPingCodec.WriteRequest(transport.Output);
        await transport.Output.FlushAsync();

        // The peer's send window is full, so the close frame can never be sent and CloseOutputAsync
        // blocks forever. Disposal must still complete within the graceful-close bound (Abort forces
        // the stuck close) rather than hang until the OS TCP timeout.
        var dispose = transport.DisposeAsync().AsTask();
        var finished = await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(10)));
        finished.ShouldBe(dispose, "disposal must not hang past the graceful-close bound when the close handshake blocks");
        await dispose;
    }

    // A WebSocket whose CloseOutputAsync never completes on its own — modeling a half-open peer with
    // a saturated send window — until Abort forces it. SendAsync accepts (the drain succeeds) and
    // ReceiveAsync blocks until the transport's lifetime token cancels.
    private sealed class BlockingCloseWebSocket : WebSocket
    {
        private readonly TaskCompletionSource _closeGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile WebSocketState _state = WebSocketState.Open;

        public override WebSocketState State => _state;

        public override WebSocketCloseStatus? CloseStatus => null;

        public override string? CloseStatusDescription => null;

        public override string? SubProtocol => "mqtt";

        public override void Abort()
        {
            _state = WebSocketState.Aborted;
            _closeGate.TrySetException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));
        }

        public override void Dispose()
        {
            _state = WebSocketState.Closed;
            _closeGate.TrySetResult();
        }

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new OperationCanceledException(cancellationToken);
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) =>
            _closeGate.Task;

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) =>
            _closeGate.Task;
    }
}
