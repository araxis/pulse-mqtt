using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Microsoft.Extensions.Time.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class ResilientMqttClientTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(15);

    private sealed class ZeroJitter : Random
    {
        public override double NextDouble() => 0.0; // zero backoff delays: reconnects run immediately
    }

    private static ResilientMqttClientOptions NewOptions(IMessageStore? messageStore = null) => new()
    {
        Connect = new MqttConnectPacket { ClientId = "resilient", KeepAliveSeconds = 0 },
        ReconnectStrategy = new BackoffReconnectStrategy(new BackoffOptions(), new DefaultReconnectDecision(), new ZeroJitter()),
        MessageStore = messageStore,
    };

    [Fact]
    public async Task Cold_start_reaches_connected_through_connecting()
    {
        var factory = new SequencedTransportFactory();
        var states = new List<ConnectionState>();
        await using var client = new ResilientMqttClient(factory, NewOptions());
        client.StateChanged += changed => { lock (states) { states.Add(changed.Current); } };

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        lock (states)
        {
            states.ShouldContain(ConnectionState.Connecting);
            states.Last().ShouldBe(ConnectionState.Connected);
        }
    }

    [Fact]
    public async Task Wait_until_connected_completes_on_connected_state()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var wait = client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        await wait;
        client.State.ShouldBe(ConnectionState.Connected);
    }

    [Fact]
    public async Task Wait_until_connected_returns_immediately_when_already_connected()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await client.WaitUntilConnectedAsync(TimeSpan.Zero, timeout.Token);
    }

    [Fact]
    public async Task Wait_until_connected_throws_when_client_faults()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var wait = client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(new MqttConnAckPacket { ReasonCode = MqttReasonCode.NotAuthorized }, timeout.Token);

        await Should.ThrowAsync<InvalidOperationException>(wait);
    }

    [Fact]
    public async Task Wait_until_connected_uses_the_client_time_provider_for_timeout()
    {
        var time = new FakeTimeProvider();
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(), time);

        var wait = client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(5));

        await Should.ThrowAsync<TimeoutException>(wait);
    }

    [Fact]
    public async Task Drop_reconnects_resubscribes_before_flushing_queued_publishes()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        // Session 1: connect and subscribe.
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var subscribeTask = client.SubscribeAsync([new MqttTopicFilter("devices/+")], timeout.Token);
        var subscribe1 = (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker1.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe1.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService2] },
            timeout.Token);
        (await subscribeTask).ShouldHaveSingleItem();

        // Drop the broker; publish while offline.
        await broker1.DisposeAsync();
        PublishOutcome queued = default;
        await WaitUntilAsync(
            async () =>
            {
                queued = await client.PublishAsync(
                    new MqttPublishPacket { Topic = "devices/1", Payload = new byte[] { 9 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
                    timeout.Token);
                return queued.Disposition == PublishDisposition.Queued;
            },
            timeout.Token);

        // Session 2: the lifecycle must re-subscribe BEFORE the queued publish flushes.
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: false);

        var resubscribe = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        resubscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("devices/+");
        await broker2.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = resubscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService2] },
            timeout.Token);

        var flushed = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        flushed.Topic.ShouldBe("devices/1");
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = flushed.PacketIdentifier!.Value },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task Terminal_rejection_faults_sticky_and_restart_recovers()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker1.SendAsync(new MqttConnAckPacket { ReasonCode = MqttReasonCode.NotAuthorized }, timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Faulted, timeout.Token);
        var connectionsAfterFault = factory.ConnectionsHandedOut;

        await Task.Delay(150, timeout.Token);
        factory.ConnectionsHandedOut.ShouldBe(connectionsAfterFault); // sticky: no auto-retry out of Faulted

        await client.ConnectAsync(timeout.Token);
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task Offline_qos0_is_dropped_and_qos1_is_queued()
    {
        var store = new InMemoryMessageStore(new OfflineQueueOptions());
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);

        var qos0 = await client.PublishAsync(new MqttPublishPacket { Topic = "t" }, timeout.Token);
        qos0.Disposition.ShouldBe(PublishDisposition.DroppedOffline);
        store.Count.ShouldBe(0);

        var qos1 = await client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        qos1.Disposition.ShouldBe(PublishDisposition.Queued);
        store.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Stop_transitions_to_stopped()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await client.DisconnectAsync(timeout.Token);

        client.State.ShouldBe(ConnectionState.Stopped);
    }

    [Fact]
    public async Task Inbound_messages_survive_reconnects()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await broker1.SendAsync(new MqttPublishPacket { Topic = "a", Payload = new byte[] { 1 } }, timeout.Token);
        (await client.Messages.ReadAsync(timeout.Token)).Topic.ShouldBe("a");

        await broker1.DisposeAsync();

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await broker2.SendAsync(new MqttPublishPacket { Topic = "b", Payload = new byte[] { 2 } }, timeout.Token);
        (await client.Messages.ReadAsync(timeout.Token)).Topic.ShouldBe("b"); // same reader, new session
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken ct)
    {
        while (client.State != state)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Delay(5, ct);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, CancellationToken ct)
    {
        while (!await condition())
        {
            ct.ThrowIfCancellationRequested();
            await Task.Delay(5, ct);
        }
    }
}
