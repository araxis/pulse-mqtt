using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class DiagnosticsSnapshotTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class ZeroJitter : Random
    {
        public override double NextDouble() => 0.0;
    }

    private static ResilientMqttClientOptions NewOptions(IMessageStore? messageStore = null) => new()
    {
        Connect = new MqttConnectPacket { ClientId = "diagnostics", KeepAliveSeconds = 0 },
        ReconnectStrategy = new BackoffReconnectStrategy(new BackoffOptions(), new DefaultReconnectDecision(), new ZeroJitter()),
        MessageStore = messageStore,
    };

    [Fact]
    public async Task Initial_snapshot_reports_disconnected_client()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.ClientId.ShouldBe("diagnostics");
        snapshot.State.ShouldBe(ConnectionState.Disconnected);
        snapshot.Attempt.ShouldBe(0);
        snapshot.IsRunning.ShouldBeFalse();
        snapshot.LastReason.ShouldBeNull();
        snapshot.LastError.ShouldBeNull();
        snapshot.OfflineQueueDepth.ShouldBe(0);
        snapshot.OfflineQueueDroppedCount.ShouldBe(0);
        snapshot.SubscriptionCount.ShouldBe(0);
        snapshot.PendingSubscribeCount.ShouldBe(0);
        snapshot.PendingUnsubscribeCount.ShouldBe(0);
    }

    [Fact]
    public async Task Connected_snapshot_reports_running_client()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.State.ShouldBe(ConnectionState.Connected);
        snapshot.IsRunning.ShouldBeTrue();
        snapshot.LastReason.ShouldBeNull();
        snapshot.LastError.ShouldBeNull();
    }

    [Fact]
    public async Task Offline_queue_counts_are_reported()
    {
        var store = new InMemoryMessageStore(new OfflineQueueOptions
        {
            Capacity = 1,
            Overflow = OverflowPolicy.DropNewest,
        });
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.PublishAsync(
            new MqttPublishPacket { Topic = "devices/1", QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        await client.PublishAsync(
            new MqttPublishPacket { Topic = "devices/2", QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);

        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.OfflineQueueDepth.ShouldBe(1);
        snapshot.OfflineQueueDroppedCount.ShouldBe(1);
    }

    [Fact]
    public async Task Pending_subscription_counts_are_reported_while_offline()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.SubscribeAsync([new MqttTopicFilter("devices/a")], timeout.Token);
        await client.SubscribeAsync([new MqttTopicFilter("devices/b")], timeout.Token);
        await client.UnsubscribeAsync(["devices/a"], timeout.Token);

        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.SubscriptionCount.ShouldBe(1);
        snapshot.PendingSubscribeCount.ShouldBe(1);
        snapshot.PendingUnsubscribeCount.ShouldBe(1);
    }

    [Fact]
    public async Task Faulted_snapshot_includes_rejection_reason_and_error()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(
            new MqttConnAckPacket
            {
                ReasonCode = MqttReasonCode.NotAuthorized,
                ReasonString = "bad credentials",
                ServerReference = "auth.example:1883",
            },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Faulted, timeout.Token);
        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.State.ShouldBe(ConnectionState.Faulted);
        snapshot.LastReason.ShouldBe(MqttReasonCode.NotAuthorized);
        snapshot.LastReasonString.ShouldBe("bad credentials");
        snapshot.LastServerReference.ShouldBe("auth.example:1883");
        snapshot.LastError.ShouldBeOfType<TerminalMqttConnectException>();
    }

    [Fact]
    public async Task Broker_disconnect_snapshot_includes_reason_string_and_server_reference()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker.SendAsync(
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ReasonString = "rebalancing",
                ServerReference = "backup.example:1883",
            },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Faulted, timeout.Token);
        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.LastReason.ShouldBe(MqttReasonCode.UseAnotherServer);
        snapshot.LastReasonString.ShouldBe("rebalancing");
        snapshot.LastServerReference.ShouldBe("backup.example:1883");
        snapshot.LastError.ShouldBeOfType<MqttServerDisconnectedException>();
    }

    [Fact]
    public async Task Queue_counter_failures_do_not_fail_snapshot_collection()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(new ThrowingCounterStore()));

        var snapshot = client.GetDiagnosticsSnapshot();

        snapshot.OfflineQueueDepth.ShouldBeNull();
        snapshot.OfflineQueueDroppedCount.ShouldBeNull();
    }

    private sealed class ThrowingCounterStore : IMessageStore
    {
        public int Count => throw new InvalidOperationException("count failed");

        public long DroppedCount => throw new InvalidOperationException("dropped failed");

        public ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult<MqttPublishPacket?>(null);

        public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask ClearAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
