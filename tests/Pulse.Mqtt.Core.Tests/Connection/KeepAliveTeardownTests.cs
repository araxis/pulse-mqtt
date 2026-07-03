using System.IO.Pipelines;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Transport;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

/// <summary>
/// The keep-alive loop's failure handling must hold for ANY transport exception: DisposeAsync
/// awaits the loop assuming it swallows its own failures, so a transport-specific exception
/// escaping the loop (QUIC's QuicException on a dead connection, for one) surfaced as a
/// teardown crash from DisposeAsync.
/// </summary>
public sealed class KeepAliveTeardownTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_transport_specific_ping_failure_does_not_escape_disposal()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var failing = new FailingWriteTransport(clientTransport);
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        var client = new RawMqttClient(new FixedTransportFactory(failing), new RawMqttClientOptions(), time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c", KeepAliveSeconds = 1 }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        // The next ping fires into a writer that throws an exception no MQTT-aware filter names.
        failing.ArmFailure();
        for (var i = 0; i < 40 && !failing.Failed; i++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(10, timeout.Token);
        }

        Assert.True(failing.Failed, "the keep-alive ping never hit the failing writer");

        // Must complete without rethrowing the transport exception from the keep-alive loop.
        await client.DisposeAsync();
    }

    private sealed class TestTransportException() : Exception("transport-specific failure");

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    private sealed class FailingWriteTransport(IMqttTransport inner) : IMqttTransport
    {
        private readonly FailingPipeWriter _output = new(inner.Output);

        public PipeReader Input => inner.Input;

        public PipeWriter Output => _output;

        public bool Failed => _output.Failed;

        public void ArmFailure() => _output.Fail = true;

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        private sealed class FailingPipeWriter(PipeWriter inner) : PipeWriter
        {
            public volatile bool Fail;
            public volatile bool Failed;

            public override void Advance(int bytes) => inner.Advance(bytes);

            public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);

            public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);

            public override void CancelPendingFlush() => inner.CancelPendingFlush();

            public override void Complete(Exception? exception = null) => inner.Complete(exception);

            public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                if (Fail)
                {
                    Failed = true;
                    throw new TestTransportException();
                }

                return inner.FlushAsync(cancellationToken);
            }
        }
    }
}
