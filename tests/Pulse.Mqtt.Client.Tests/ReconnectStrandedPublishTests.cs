using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>
/// Regression coverage for the reconnect strand: a QoS 1 publish racing a reconnect could
/// deposit its message into the offline queue after the connection-up flush had already
/// scanned, leaving the client Connected with the message stuck in the queue forever — the
/// long-standing chaos-suite "exactly one message missing" flake.
/// </summary>
public sealed class ReconnectStrandedPublishTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task An_enqueue_landing_after_the_reconnect_flush_is_still_delivered()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var factory = new SequencedTransportFactory();
        var store = new GatedMessageStore();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = "strand-regression",
                KeepAliveSeconds = 0,
                CleanStart = false,
                SessionExpiryInterval = 300,
            },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(5) },
            MessageStore = store,
        });

        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // Kill connection 1 (the chaos kill) and wait for the second connect attempt to start —
        // the publish below then observes no live connection.
        await broker1.DisposeAsync();
        while (factory.ConnectionsHandedOut < 2)
        {
            await Task.Delay(5, timeout.Token);
        }

        // The publish blocks at the enqueue boundary (modeling a thread-pool scheduling delay)
        // while the reconnect completes fully: session resumed, redelivery, flush of the — still
        // empty — queue, state Connected. Only then does the enqueue land.
        store.Arm();
        var publishTask = client.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "chaos/strand",
                Payload = Encoding.UTF8.GetBytes("23"),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);
        await store.EnqueueEntered.WaitAsync(timeout.Token);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        store.Release();

        // The post-enqueue re-check must nudge the flush: the broker receives the publish and
        // the queue drains. Before the fix, nothing arrived and the queue held the message
        // forever while the client reported Connected.
        var brokerSide = Task.Run(async () =>
        {
            var packet = await broker2.ReadPacketAsync(timeout.Token);
            var publish = packet.ShouldBeOfTypeOrThrow<MqttPublishPacket>();
            Encoding.UTF8.GetString(publish.Payload.ToArray()).ShouldBe("23");
            await broker2.SendAsync(
                new MqttPublishAckPacket
                {
                    PacketType = MqttPacketType.PubAck,
                    PacketIdentifier = publish.PacketIdentifier!.Value,
                },
                timeout.Token);
        }, timeout.Token);

        var outcome = await publishTask;
        outcome.Disposition.ShouldBe(PublishDisposition.Queued);
        await brokerSide.WaitAsync(timeout.Token);

        client.State.ShouldBe(ConnectionState.Connected);
        store.Count.ShouldBe(0, "the nudged flush must drain the raced enqueue");
    }

    [Fact]
    public async Task A_transport_failure_during_the_post_enqueue_flush_does_not_escape_the_publish()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var factory = new FlushFailingTransportFactory(failOnConnection: 2);
        var store = new GatedMessageStore();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = "strand-flush-fail",
                KeepAliveSeconds = 0,
                CleanStart = false,
                SessionExpiryInterval = 300,
            },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(5) },
            MessageStore = store,
            OfflineQueue = new OfflineQueueOptions { IncludeQos0 = true },
        });

        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker1.DisposeAsync();
        while (factory.ConnectionsHandedOut < 2)
        {
            await Task.Delay(5, timeout.Token);
        }

        // A QoS 0 publish races the reconnect: it enqueues offline, and the post-enqueue nudge
        // flush drains it against the revived connection 2 — whose writer is armed to throw a raw
        // transport exception (IOException). That exception must NOT escape PublishAsync, because
        // the caller's message is already queued (outcome Queued); the next connection-up flush
        // will drain it.
        store.Arm();
        var publishTask = client.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "chaos/flush-fail",
                Payload = Encoding.UTF8.GetBytes("23"),
                QualityOfService = MqttQualityOfService.AtMostOnce,
            },
            timeout.Token);
        await store.EnqueueEntered.WaitAsync(timeout.Token);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        factory.ArmFailure(); // connection 2's writer now throws IOException on flush
        store.Release();

        var outcome = await publishTask; // before the fix this threw the IOException
        outcome.Disposition.ShouldBe(PublishDisposition.Queued);
        store.Count.ShouldBe(1, "the failed flush leaves the message queued for the next connection-up flush");
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken token)
    {
        while (client.State != state)
        {
            await Task.Delay(5, token);
        }
    }

    /// <summary>Delays one enqueue at the await boundary, on demand — the scheduling gap that opens the strand window.</summary>
    private sealed class GatedMessageStore : IMessageStore
    {
        private readonly InMemoryMessageStore _inner = new(new OfflineQueueOptions());
        private readonly SemaphoreSlim _entered = new(0);
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile bool _armed;

        public SemaphoreSlim EnqueueEntered => _entered;

        public int Count => _inner.Count;

        public long DroppedCount => _inner.DroppedCount;

        public void Arm() => _armed = true;

        public void Release() => _gate.TrySetResult();

        public async ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken)
        {
            await PauseIfArmedAsync();
            await _inner.EnqueueAsync(packet, cancellationToken);
        }

        public async ValueTask EnqueueAsync(MqttPublishPacket packet, DateTimeOffset enqueuedAt, CancellationToken cancellationToken)
        {
            await PauseIfArmedAsync();
            await _inner.EnqueueAsync(packet, enqueuedAt, cancellationToken);
        }

        public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) => _inner.PeekAsync(cancellationToken);

        public ValueTask<MqttQueuedPublish?> PeekQueuedAsync(CancellationToken cancellationToken) => _inner.PeekQueuedAsync(cancellationToken);

        public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) => _inner.RemoveHeadAsync(cancellationToken);

        public ValueTask ClearAsync(CancellationToken cancellationToken) => _inner.ClearAsync(cancellationToken);

        private async Task PauseIfArmedAsync()
        {
            if (_armed)
            {
                _armed = false;
                _entered.Release();
                await _gate.Task;
            }
        }
    }

    /// <summary>
    /// Hands out loopback connections like <see cref="SequencedTransportFactory"/>, but wraps a
    /// chosen connection's writer so it can be armed to throw a raw transport exception on flush —
    /// modeling a connection that dies the moment the post-enqueue flush writes to it.
    /// </summary>
    private sealed class FlushFailingTransportFactory(int failOnConnection) : IMqttTransportFactory
    {
        private readonly Channel<TestBroker> _brokers = Channel.CreateUnbounded<TestBroker>();
        private ArmablePipeWriter? _writer;

        public int ConnectionsHandedOut { get; private set; }

        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
        {
            var (client, server) = LoopbackTransport.CreatePair();
            _brokers.Writer.TryWrite(new TestBroker(server));
            ConnectionsHandedOut++;

            if (ConnectionsHandedOut == failOnConnection)
            {
                _writer = new ArmablePipeWriter(client.Output);
                return ValueTask.FromResult<IMqttTransport>(new WrappedTransport(client, _writer));
            }

            return ValueTask.FromResult(client);
        }

        public ValueTask<TestBroker> NextBrokerAsync(CancellationToken cancellationToken) =>
            _brokers.Reader.ReadAsync(cancellationToken);

        // Arm only after the handshake so the connect itself succeeds; the next flush then throws.
        public void ArmFailure() => (_writer ?? throw new InvalidOperationException("The failing connection was not handed out yet.")).Armed = true;
    }

    private sealed class WrappedTransport(IMqttTransport inner, PipeWriter output) : IMqttTransport
    {
        public PipeReader Input => inner.Input;

        public PipeWriter Output => output;

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private sealed class ArmablePipeWriter(PipeWriter inner) : PipeWriter
    {
        public volatile bool Armed;

        public override void Advance(int bytes) => inner.Advance(bytes);

        public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);

        public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);

        public override void CancelPendingFlush() => inner.CancelPendingFlush();

        public override void Complete(Exception? exception = null) => inner.Complete(exception);

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) =>
            Armed ? throw new IOException("The connection died while the post-enqueue flush was writing.") : inner.FlushAsync(cancellationToken);
    }
}
