using System.Text;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
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
}
