using System.IO.Pipelines;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

/// <summary>
/// Tearing down a client whose transport died must not throw. The exception a dead transport
/// raises from the send path is the transport's choice — QUIC throws <see cref="IOException"/>-derived
/// QuicException, sockets throw SocketException — so teardown code cannot enumerate the types.
/// The regression: the keep-alive loop's final catch listed only <see cref="MqttException"/> and
/// <see cref="ObjectDisposedException"/>, so a transport exception thrown mid-PINGREQ faulted the
/// keep-alive task and <see cref="RawMqttClient.DisposeAsync"/> rethrew it at the await; the
/// best-effort DISCONNECT send in <see cref="RawMqttClient.DisconnectAsync"/> had the same filter
/// and leaked the same way.
/// </summary>
public sealed class TransportFailureTeardownTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    [Fact]
    public async Task Disposing_a_client_whose_transport_died_mid_ping_does_not_throw()
    {
        var (client, transport, time, ct) = await ConnectedAsync(keepAliveSeconds: 5);

        // The transport dies; the next keep-alive PINGREQ fails with a transport-chosen exception.
        transport.FailSendsWith(new IOException("Connection aborted by peer (1)."));
        await AdvanceUntilAsync(time, () => transport.SendFailed.Task.IsCompleted);
        await transport.SendFailed.Task.WaitAsync(SafetyTimeout, ct);

        await Should.NotThrowAsync(async () => await client.DisposeAsync());
    }

    [Fact]
    public async Task Disconnecting_a_client_whose_transport_died_does_not_throw()
    {
        var (client, transport, _, ct) = await ConnectedAsync(keepAliveSeconds: 0);

        // The transport dies; the best-effort DISCONNECT fails with a transport-chosen exception.
        transport.FailSendsWith(new IOException("Connection aborted by peer (1)."));

        await Should.NotThrowAsync(async () => await client.DisconnectAsync(ct));
    }

    private static async Task<(RawMqttClient Client, FaultableTransport Transport, FakeTimeProvider Time, CancellationToken Ct)> ConnectedAsync(
        ushort keepAliveSeconds)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var transport = new FaultableTransport(clientTransport);
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        var client = new RawMqttClient(new FixedTransportFactory(transport), new RawMqttClientOptions(), time);

        var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(
            new MqttConnectPacket { ClientId = "c", KeepAliveSeconds = keepAliveSeconds }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        return (client, transport, time, timeout.Token);
    }

    private static async Task AdvanceUntilAsync(FakeTimeProvider time, Func<bool> condition)
    {
        for (var i = 0; i < 400 && !condition(); i++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(5);
        }
    }

    // A transport that sends normally until armed, then throws the given exception from every
    // flush — the shape of a QUIC/TCP connection dying between the handshake and a later send.
    private sealed class FaultableTransport(IMqttTransport inner) : IMqttTransport
    {
        private readonly FaultableWriter _output = new(inner.Output);

        public TaskCompletionSource SendFailed => _output.SendFailed;

        public PipeReader Input => inner.Input;

        public PipeWriter Output => _output;

        public void FailSendsWith(Exception error) => _output.Arm(error);

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        private sealed class FaultableWriter(PipeWriter inner) : PipeWriter
        {
            private volatile Exception? _error;

            public TaskCompletionSource SendFailed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public void Arm(Exception error) => _error = error;

            public override void Advance(int bytes) => inner.Advance(bytes);

            public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);

            public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);

            public override void CancelPendingFlush() => inner.CancelPendingFlush();

            public override void Complete(Exception? exception = null) => inner.Complete(exception);

            public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                if (_error is { } error)
                {
                    SendFailed.TrySetResult();
                    throw error;
                }

                return inner.FlushAsync(cancellationToken);
            }
        }
    }
}
