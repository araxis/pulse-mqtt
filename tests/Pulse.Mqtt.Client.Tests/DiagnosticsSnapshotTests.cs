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

    private static ResilientMqttClientOptions NewOptions(
        IMessageStore? messageStore = null,
        MqttProtocolVersion protocolVersion = MqttProtocolVersion.V500,
        ushort keepAliveSeconds = 0) => new()
    {
        Connect = new MqttConnectPacket
        {
            ClientId = "diagnostics",
            KeepAliveSeconds = keepAliveSeconds,
            ProtocolVersion = protocolVersion,
        },
        ReconnectStrategy = new BackoffReconnectStrategy(
            new BackoffOptions
            {
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(10),
            },
            new DefaultReconnectDecision(),
            new ZeroJitter()),
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
        snapshot.BrokerCapabilities.ShouldBeNull();
        client.GetBrokerCapabilitiesSnapshot().ShouldBeNull();
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
        snapshot.BrokerCapabilities.ShouldNotBeNull();
        client.GetBrokerCapabilitiesSnapshot().ShouldBeSameAs(snapshot.BrokerCapabilities);
    }

    [Fact]
    public async Task Connected_mqtt5_snapshot_includes_raw_values_and_effective_defaults()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(keepAliveSeconds: 30));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(
            new MqttConnAckPacket
            {
                SessionPresent = true,
                AssignedClientIdentifier = "assigned-client",
                ReceiveMaximum = 10,
                MaximumQoS = MqttQualityOfService.AtLeastOnce,
                RetainAvailable = true,
                MaximumPacketSize = 4096,
                TopicAliasMaximum = 3,
                SharedSubscriptionAvailable = true,
                ServerKeepAlive = 7,
                ResponseInformation = "responses",
                ServerReference = "broker-a",
                AuthenticationMethod = "token",
            },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        var capabilities = client.GetBrokerCapabilitiesSnapshot().ShouldNotBeNull();
        var diagnostics = client.GetDiagnosticsSnapshot();

        diagnostics.BrokerCapabilities.ShouldBeSameAs(capabilities);
        capabilities.ProtocolVersion.ShouldBe(MqttProtocolVersion.V500);
        capabilities.SessionPresent.ShouldBeTrue();
        capabilities.AssignedClientIdentifier.ShouldBe("assigned-client");
        capabilities.ReceiveMaximum.ShouldBe((ushort)10);
        capabilities.EffectiveReceiveMaximum.ShouldBe((ushort)10);
        capabilities.MaximumQoS.ShouldBe(MqttQualityOfService.AtLeastOnce);
        capabilities.EffectiveMaximumQoS.ShouldBe(MqttQualityOfService.AtLeastOnce);
        capabilities.RetainAvailable.ShouldBe(true);
        capabilities.RetainedMessages.ShouldBe(MqttBrokerFeatureSupport.Supported);
        capabilities.MaximumPacketSize.ShouldBe(4096u);
        capabilities.TopicAliasMaximum.ShouldBe((ushort)3);
        capabilities.EffectiveTopicAliasMaximum.ShouldBe((ushort)3);
        capabilities.TopicAliases.ShouldBe(MqttBrokerFeatureSupport.Supported);
        capabilities.WildcardSubscriptionAvailable.ShouldBeNull();
        capabilities.WildcardSubscriptions.ShouldBe(MqttBrokerFeatureSupport.Supported);
        capabilities.SubscriptionIdentifiersAvailable.ShouldBeNull();
        capabilities.SubscriptionIdentifiers.ShouldBe(MqttBrokerFeatureSupport.Supported);
        capabilities.SharedSubscriptionAvailable.ShouldBe(true);
        capabilities.SharedSubscriptions.ShouldBe(MqttBrokerFeatureSupport.Supported);
        capabilities.ServerKeepAlive.ShouldBe((ushort)7);
        capabilities.EffectiveKeepAliveSeconds.ShouldBe((ushort)7);
        capabilities.ResponseInformation.ShouldBe("responses");
        capabilities.ServerReference.ShouldBe("broker-a");
        capabilities.AuthenticationMethod.ShouldBe("token");
    }

    [Fact]
    public async Task Mqtt5_explicit_unsupported_flags_map_to_not_supported()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(keepAliveSeconds: 30));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(
            new MqttConnAckPacket
            {
                RetainAvailable = false,
                WildcardSubscriptionAvailable = false,
                SubscriptionIdentifiersAvailable = false,
                SharedSubscriptionAvailable = false,
            },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        var capabilities = client.GetBrokerCapabilitiesSnapshot().ShouldNotBeNull();

        capabilities.EffectiveReceiveMaximum.ShouldBe(ushort.MaxValue);
        capabilities.EffectiveMaximumQoS.ShouldBe(MqttQualityOfService.ExactlyOnce);
        capabilities.RetainedMessages.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.WildcardSubscriptions.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.SubscriptionIdentifiers.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.SharedSubscriptions.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.EffectiveTopicAliasMaximum.ShouldBe((ushort)0);
        capabilities.TopicAliases.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.EffectiveKeepAliveSeconds.ShouldBe((ushort)30);
    }

    [Fact]
    public async Task Mqtt311_snapshot_marks_unnegotiated_support_as_unknown()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(
            factory,
            NewOptions(protocolVersion: MqttProtocolVersion.V311, keepAliveSeconds: 45));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(
            new MqttConnAckPacket { ProtocolVersion = MqttProtocolVersion.V311 },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        var capabilities = client.GetBrokerCapabilitiesSnapshot().ShouldNotBeNull();

        capabilities.ProtocolVersion.ShouldBe(MqttProtocolVersion.V311);
        capabilities.ReceiveMaximum.ShouldBeNull();
        capabilities.EffectiveReceiveMaximum.ShouldBeNull();
        capabilities.RetainedMessages.ShouldBe(MqttBrokerFeatureSupport.Unknown);
        capabilities.WildcardSubscriptions.ShouldBe(MqttBrokerFeatureSupport.Unknown);
        capabilities.SubscriptionIdentifiers.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.SharedSubscriptions.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.TopicAliases.ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        capabilities.EffectiveTopicAliasMaximum.ShouldBe((ushort)0);
        capabilities.EffectiveMaximumQoS.ShouldBe(MqttQualityOfService.ExactlyOnce);
        capabilities.EffectiveKeepAliveSeconds.ShouldBe((ushort)45);
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
        snapshot.BrokerCapabilities.ShouldBeNull();
        client.GetBrokerCapabilitiesSnapshot().ShouldBeNull();
    }

    [Fact]
    public async Task Disconnect_clears_broker_capabilities()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        client.GetBrokerCapabilitiesSnapshot().ShouldNotBeNull();

        await client.DisconnectAsync(timeout.Token);

        client.State.ShouldBe(ConnectionState.Stopped);
        client.GetBrokerCapabilitiesSnapshot().ShouldBeNull();
        client.GetDiagnosticsSnapshot().BrokerCapabilities.ShouldBeNull();
    }

    [Fact]
    public async Task Reconnect_replaces_broker_capabilities()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var firstBroker = await factory.NextBrokerAsync(timeout.Token);
        (await firstBroker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await firstBroker.SendAsync(
            new MqttConnAckPacket { TopicAliasMaximum = 1 },
            timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        client.GetBrokerCapabilitiesSnapshot().ShouldNotBeNull().TopicAliasMaximum.ShouldBe((ushort)1);

        await firstBroker.DisposeAsync();
        var secondBroker = await factory.NextBrokerAsync(timeout.Token);
        (await secondBroker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await secondBroker.SendAsync(
            new MqttConnAckPacket
            {
                TopicAliasMaximum = 5,
                ReceiveMaximum = 25,
            },
            timeout.Token);

        while (client.GetBrokerCapabilitiesSnapshot()?.TopicAliasMaximum != 5)
        {
            await Task.Delay(1, timeout.Token);
        }

        var capabilities = client.GetBrokerCapabilitiesSnapshot().ShouldNotBeNull();
        capabilities.TopicAliasMaximum.ShouldBe((ushort)5);
        capabilities.EffectiveReceiveMaximum.ShouldBe((ushort)25);
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
