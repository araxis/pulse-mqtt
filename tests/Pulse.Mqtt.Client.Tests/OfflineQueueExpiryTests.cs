using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class OfflineQueueExpiryTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Messages_that_expired_while_offline_are_not_flushed()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryMessageStore(new OfflineQueueOptions());

        // Three publishes queued while offline: one that will expire during the wait, one whose
        // expiry outlives it, and one without expiry.
        var queuedAt = time.GetUtcNow();
        await store.EnqueueAsync(
            new MqttPublishPacket { Topic = "t/expired", Payload = "a"u8.ToArray(), MessageExpiryInterval = 30 }, queuedAt, CancellationToken.None);
        await store.EnqueueAsync(
            new MqttPublishPacket { Topic = "t/survives", Payload = "b"u8.ToArray(), MessageExpiryInterval = 3600 }, queuedAt, CancellationToken.None);
        await store.EnqueueAsync(
            new MqttPublishPacket { Topic = "t/eternal", Payload = "c"u8.ToArray() }, queuedAt, CancellationToken.None);

        // 60 seconds pass while offline — past the 30s expiry, well within the 3600s one.
        time.Advance(TimeSpan.FromSeconds(60));

        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(
            factory,
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = "expiry", KeepAliveSeconds = 0 },
                MessageStore = store,
            },
            timeProvider: time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        // The flush must skip the expired message entirely and shorten the surviving one's
        // remaining interval by the time it waited.
        var first = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        first.Topic.ShouldBe("t/survives");
        first.MessageExpiryInterval.ShouldBe(3600u - 60u);

        var second = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        second.Topic.ShouldBe("t/eternal");
        second.MessageExpiryInterval.ShouldBeNull();

        await client.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task A_message_on_the_expiry_boundary_is_dropped()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryMessageStore(new OfflineQueueOptions());
        await store.EnqueueAsync(
            new MqttPublishPacket { Topic = "t", Payload = "x"u8.ToArray(), MessageExpiryInterval = 30 },
            time.GetUtcNow(),
            CancellationToken.None);

        time.Advance(TimeSpan.FromSeconds(30));

        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(
            factory,
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = "boundary", KeepAliveSeconds = 0 },
                MessageStore = store,
            },
            timeProvider: time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        // Exactly-elapsed expiry means expired: the queue drains without sending, so a PINGREQ
        // scripted from a fresh publish proves nothing else arrived first.
        await WaitUntilAsync(() => store.Count == 0, timeout.Token);

        await client.PublishAsync(new MqttPublishPacket { Topic = "t/after", Payload = "y"u8.ToArray() }, timeout.Token);
        var observed = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        observed.Topic.ShouldBe("t/after");

        await client.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task Stores_without_enqueue_stamps_flush_without_expiry_accounting()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryMessageStore(new OfflineQueueOptions());
        // The legacy overload carries no stamp: even an "expired" message flushes untouched,
        // because there is no wait time to charge against it.
        await store.EnqueueAsync(
            new MqttPublishPacket { Topic = "t/untracked", Payload = "x"u8.ToArray(), MessageExpiryInterval = 30 },
            CancellationToken.None);

        time.Advance(TimeSpan.FromSeconds(3600));

        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(
            factory,
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = "untracked", KeepAliveSeconds = 0 },
                MessageStore = store,
            },
            timeProvider: time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        var flushed = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        flushed.Topic.ShouldBe("t/untracked");
        flushed.MessageExpiryInterval.ShouldBe(30u);

        await client.DisconnectAsync(timeout.Token);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
