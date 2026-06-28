using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Testing;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.DependencyInjection.Tests;

public sealed class AddPulseMqttClientTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class ZeroJitter : Random
    {
        public override double NextDouble() => 0.0;
    }

    private sealed class LoopbackOnlyFactory : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
        {
            var (client, _) = LoopbackTransport.CreatePair();
            return ValueTask.FromResult(client);
        }
    }

    private sealed class StaticWillProvider(string topic) : IMqttWillProvider
    {
        public ValueTask<MqttWillMessage?> CreateWillAsync(
            MqttWillContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<MqttWillMessage?>(new MqttWillMessage(topic));
    }

    private sealed class GenericWillProvider : IMqttWillProvider
    {
        public ValueTask<MqttWillMessage?> CreateWillAsync(
            MqttWillContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<MqttWillMessage?>(new MqttWillMessage($"status/{context.ClientId}/generic"));
    }

    private static void ValidOptions(PulseMqttClientOptions options)
    {
        options.Host = "broker.local";
        options.ClientId = "test-client";
    }

    private static ResilientMqttClientOptions NewClientOptions(IMessageStore? messageStore = null) => new()
    {
        Connect = new MqttConnectPacket
        {
            ClientId = $"health-{Guid.NewGuid():N}",
            KeepAliveSeconds = 0,
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
    public async Task Named_clients_are_independent_and_cached()
    {
        var services = new ServiceCollection();
        services.AddPulseMqttClient("a", ValidOptions);
        services.AddPulseMqttClient("b", ValidOptions);
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IPulseMqttClientFactory>();

        var a1 = factory.GetClient("a");
        var a2 = factory.GetClient("a");
        var b = factory.GetClient("b");

        a1.ShouldBeSameAs(a2);
        a1.ShouldNotBeSameAs(b);
    }

    [Fact]
    public async Task A_swapped_session_store_is_used_by_the_client()
    {
        var store = new InMemorySessionStore();
        var services = new ServiceCollection();
        services.AddPulseMqttClient("swap", ValidOptions)
            .UseSessionStore(_ => store);
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("swap");
        await client.SubscribeAsync([new MqttTopicFilter("a/+")], CancellationToken.None); // offline: persists only

        var saved = await store.LoadSubscriptionsAsync(CancellationToken.None);
        saved.ShouldHaveSingleItem().Topic.ShouldBe("a/+");
    }

    [Fact]
    public async Task A_registered_will_provider_is_used_by_the_named_client()
    {
        MqttConnectPacket? capturedConnect = null;
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            ConnAckFactory = context =>
            {
                capturedConnect = context.Connect;
                return context.DefaultConnAck;
            },
        });
        var services = new ServiceCollection();
        services.AddPulseMqttClient("will-provider", options =>
        {
            ValidOptions(options);
            options.Will = new PulseMqttWillOptions
            {
                Topic = "status/static",
                Payload = "offline",
            };
        })
            .UseTransportFactory(_ => broker)
            .UseWillFactory(_ => _ => ValueTask.FromResult(new MqttWillMessage("status/factory")))
            .UseWillProvider(_ => new StaticWillProvider("status/provider"));
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("will-provider");
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        capturedConnect.ShouldNotBeNull().Will.ShouldNotBeNull().Topic.ShouldBe("status/provider");
    }

    [Fact]
    public async Task A_generic_will_provider_registration_is_used_by_the_named_client()
    {
        MqttConnectPacket? capturedConnect = null;
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            ConnAckFactory = context =>
            {
                capturedConnect = context.Connect;
                return context.DefaultConnAck;
            },
        });
        var services = new ServiceCollection();
        services.AddPulseMqttClient("generic-will", ValidOptions)
            .UseTransportFactory(_ => broker)
            .UseWillProvider<GenericWillProvider>();
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("generic-will");
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        capturedConnect.ShouldNotBeNull().Will.ShouldNotBeNull().Topic.ShouldBe("status/test-client/generic");
    }

    [Theory]
    [InlineData("", "client")]
    [InlineData("host", "")]
    public void Invalid_options_fail_at_first_resolve_with_a_clear_message(string host, string clientId)
    {
        var services = new ServiceCollection();
        services.AddPulseMqttClient("bad", o => { o.Host = host; o.ClientId = clientId; });
        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IPulseMqttClientFactory>();

        var thrown = Should.Throw<InvalidOperationException>(() => factory.GetClient("bad"));
        thrown.Message.ShouldContain("bad");
    }

    [Fact]
    public async Task The_hosted_service_starts_and_stops_the_client()
    {
        var services = new ServiceCollection();
        services.AddPulseMqttClient("hosted", ValidOptions)
            .UseTransportFactory(_ => new LoopbackOnlyFactory());
        await using var provider = services.BuildServiceProvider();

        var hosted = provider.GetRequiredService<IHostedService>();
        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("hosted");
        client.State.ShouldBe(ConnectionState.Disconnected);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await hosted.StartAsync(timeout.Token);
        while (client.State == ConnectionState.Disconnected)
        {
            timeout.Token.ThrowIfCancellationRequested();
            await Task.Delay(5, timeout.Token);
        }

        await hosted.StopAsync(timeout.Token);
        client.State.ShouldBe(ConnectionState.Stopped);
    }

    [Fact]
    public async Task A_manually_controlled_client_ignores_host_start_and_restarts_on_demand()
    {
        var services = new ServiceCollection();
        services.AddPulseMqttClient("manual", options =>
        {
            ValidOptions(options);
            options.ConnectWithHost = false;
        }).UseTransportFactory(_ => new LoopbackOnlyFactory());
        await using var provider = services.BuildServiceProvider();

        var hosted = provider.GetRequiredService<IHostedService>();
        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("manual");

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await hosted.StartAsync(timeout.Token);
        client.State.ShouldBe(ConnectionState.Disconnected); // the host did not start it

        // The application starts, stops, and restarts the client whenever it wants.
        await client.ConnectAsync(timeout.Token);
        while (client.State == ConnectionState.Disconnected)
        {
            await Task.Delay(5, timeout.Token);
        }

        await client.DisconnectAsync(timeout.Token);
        client.State.ShouldBe(ConnectionState.Stopped);

        await client.ConnectAsync(timeout.Token);
        while (client.State == ConnectionState.Stopped)
        {
            await Task.Delay(5, timeout.Token);
        }

        // Host shutdown still stops a running client.
        await hosted.StopAsync(timeout.Token);
        client.State.ShouldBe(ConnectionState.Stopped);
    }

    [Fact]
    public async Task The_health_check_tracks_the_connection_state()
    {
        var services = new ServiceCollection();
        services.AddPulseMqttClient("health", ValidOptions)
            .UseTransportFactory(_ => new LoopbackOnlyFactory())
            .AddHealthCheck();
        await using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        var registration = registrations.ShouldHaveSingleItem();
        registration.Name.ShouldBe("pulse-mqtt-health");

        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("health");
        var check = new PulseMqttHealthCheck(client);
        var context = new HealthCheckContext { Registration = registration };

        var disconnected = await check.CheckHealthAsync(context);
        disconnected.Status.ShouldBe(HealthStatus.Unhealthy); // Disconnected
        disconnected.Data["client.id"].ShouldBe("test-client");
        disconnected.Data["state"].ShouldBe(ConnectionState.Disconnected.ToString());
        disconnected.Data["attempt"].ShouldBe(0);
        disconnected.Data["offline.queue.depth"].ShouldBe(0);
        disconnected.Data["offline.queue.dropped"].ShouldBe(0L);
        disconnected.Data.ContainsKey("broker.protocol.version").ShouldBeFalse();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        while (client.State == ConnectionState.Disconnected)
        {
            await Task.Delay(5, timeout.Token);
        }

        var connecting = await check.CheckHealthAsync(context);
        connecting.Status.ShouldBe(HealthStatus.Degraded); // connecting/retrying
        connecting.Data["state"].ShouldBe(client.GetDiagnosticsSnapshot().State.ToString());
        connecting.Data.ContainsKey("state.changed_at").ShouldBeTrue();

        await client.DisconnectAsync(timeout.Token);
        var stopped = await check.CheckHealthAsync(context);
        stopped.Status.ShouldBe(HealthStatus.Unhealthy); // Stopped
        stopped.Data["state"].ShouldBe(ConnectionState.Stopped.ToString());
        stopped.Data.ContainsKey("broker.protocol.version").ShouldBeFalse();
    }

    [Fact]
    public async Task Health_check_includes_broker_capabilities_only_when_connected()
    {
        await using var broker = new PulseMqttTestBroker();
        var services = new ServiceCollection();
        services.AddPulseMqttClient("capabilities", ValidOptions)
            .UseTransportFactory(_ => broker)
            .AddHealthCheck();
        await using var provider = services.BuildServiceProvider();

        var registration = provider
            .GetRequiredService<IOptions<HealthCheckServiceOptions>>()
            .Value
            .Registrations
            .ShouldHaveSingleItem();
        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("capabilities");
        var check = new PulseMqttHealthCheck(client);
        var context = new HealthCheckContext { Registration = registration };

        var disconnected = await check.CheckHealthAsync(context);
        disconnected.Data.ContainsKey("broker.protocol.version").ShouldBeFalse();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        var connected = await check.CheckHealthAsync(context);
        connected.Status.ShouldBe(HealthStatus.Healthy);
        connected.Data["broker.protocol.version"].ShouldBe(MqttProtocolVersion.V500.ToString());
        connected.Data["broker.session.present"].ShouldBe(false);
        connected.Data["broker.receive.maximum.effective"].ShouldBe(ushort.MaxValue);
        connected.Data["broker.maximum.qos.effective"].ShouldBe(MqttQualityOfService.ExactlyOnce.ToString());
        connected.Data["broker.retained.messages"].ShouldBe(MqttBrokerFeatureSupport.Supported.ToString());
        connected.Data["broker.topic.alias.maximum.effective"].ShouldBe((ushort)0);
        connected.Data["broker.topic.aliases"].ShouldBe(MqttBrokerFeatureSupport.NotSupported.ToString());

        await client.DisconnectAsync(timeout.Token);
        var stopped = await check.CheckHealthAsync(context);
        stopped.Data.ContainsKey("broker.protocol.version").ShouldBeFalse();
    }

    [Fact]
    public async Task Builder_health_check_overload_registers_configured_thresholds()
    {
        await using var broker = new PulseMqttTestBroker();
        var store = new StaticMessageStore { Count = 2 };
        var services = new ServiceCollection();
        services.AddPulseMqttClient("policy", ValidOptions)
            .UseTransportFactory(_ => broker)
            .UseMessageStore(_ => store)
            .AddHealthCheck(options => options.DegradedOfflineQueueDepthThreshold = 2);
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("policy");
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var registration = provider
            .GetRequiredService<IOptions<HealthCheckServiceOptions>>()
            .Value
            .Registrations
            .ShouldHaveSingleItem();
        var check = registration.Factory(provider);
        var entry = await check.CheckHealthAsync(new HealthCheckContext { Registration = registration }, timeout.Token);

        registration.Name.ShouldBe("pulse-mqtt-policy");
        entry.Status.ShouldBe(HealthStatus.Degraded);
        entry.Data["health.policy.status"].ShouldBe(HealthStatus.Degraded.ToString());
        entry.Data["health.policy.reasons"].ShouldBeOfType<string[]>()
            .ShouldContain("offline.queue.depth 2 >= 2");
    }

    [Fact]
    public async Task Connected_client_below_thresholds_stays_healthy()
    {
        await using var broker = new PulseMqttTestBroker();
        var store = new StaticMessageStore { Count = 1, DroppedCount = 1 };
        await using var client = new ResilientMqttClient(broker, NewClientOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions
            {
                DegradedOfflineQueueDepthThreshold = 2,
                UnhealthyOfflineQueueDepthThreshold = 4,
                DegradedOfflineQueueDroppedCountThreshold = 2,
                UnhealthyOfflineQueueDroppedCountThreshold = 4,
            });

        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Healthy);
        result.Data.ContainsKey("health.policy.status").ShouldBeFalse();
    }

    [Fact]
    public async Task Connected_client_crossing_degraded_queue_threshold_returns_degraded()
    {
        await using var broker = new PulseMqttTestBroker();
        var store = new StaticMessageStore { Count = 2 };
        await using var client = new ResilientMqttClient(broker, NewClientOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions { DegradedOfflineQueueDepthThreshold = 2 });
        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Degraded);
        result.Data["health.policy.status"].ShouldBe(HealthStatus.Degraded.ToString());
        result.Data["health.policy.reasons"].ShouldBeOfType<string[]>()
            .ShouldContain("offline.queue.depth 2 >= 2");
    }

    [Fact]
    public async Task Connected_client_crossing_unhealthy_queue_threshold_returns_unhealthy()
    {
        await using var broker = new PulseMqttTestBroker();
        var store = new StaticMessageStore { Count = 5 };
        await using var client = new ResilientMqttClient(broker, NewClientOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions
            {
                DegradedOfflineQueueDepthThreshold = 2,
                UnhealthyOfflineQueueDepthThreshold = 5,
            });
        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Data["health.policy.status"].ShouldBe(HealthStatus.Unhealthy.ToString());
        result.Data["health.policy.reasons"].ShouldBeOfType<string[]>()
            .ShouldContain("offline.queue.depth 5 >= 5");
    }

    [Fact]
    public async Task Dropped_count_thresholds_work_independently_from_queue_depth()
    {
        await using var broker = new PulseMqttTestBroker();
        var store = new StaticMessageStore { Count = 0, DroppedCount = 3 };
        await using var client = new ResilientMqttClient(broker, NewClientOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions { DegradedOfflineQueueDroppedCountThreshold = 3 });
        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Degraded);
        result.Data["health.policy.reasons"].ShouldBeOfType<string[]>()
            .ShouldContain("offline.queue.dropped 3 >= 3");
    }

    [Fact]
    public async Task Pending_subscription_threshold_uses_combined_pending_count()
    {
        var services = new ServiceCollection();
        services.AddPulseMqttClient("pending", ValidOptions)
            .UseTransportFactory(_ => new LoopbackOnlyFactory())
            .AddHealthCheck();
        await using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("pending");

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.SubscribeAsync([new MqttTopicFilter("devices/a")], timeout.Token);
        await client.SubscribeAsync([new MqttTopicFilter("devices/b")], timeout.Token);
        await client.UnsubscribeAsync(["devices/a"], timeout.Token);
        await client.ConnectAsync(timeout.Token);
        await WaitForNotDisconnectedAsync(client, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions { UnhealthyPendingSubscriptionOperationsThreshold = 2 });
        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Data["pending.subscribe.count"].ShouldBe(1);
        result.Data["pending.unsubscribe.count"].ShouldBe(1);
        result.Data["health.policy.reasons"].ShouldBeOfType<string[]>()
            .ShouldContain("pending.subscription.operations 2 >= 2");
    }

    [Fact]
    public async Task Unhealthy_threshold_takes_precedence_when_multiple_thresholds_are_crossed()
    {
        await using var broker = new PulseMqttTestBroker();
        var store = new StaticMessageStore { Count = 2, DroppedCount = 4 };
        await using var client = new ResilientMqttClient(broker, NewClientOptions(store));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions
            {
                DegradedOfflineQueueDepthThreshold = 2,
                UnhealthyOfflineQueueDroppedCountThreshold = 4,
            });
        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Data["health.policy.status"].ShouldBe(HealthStatus.Unhealthy.ToString());
        result.Data["health.policy.reasons"].ShouldBeOfType<string[]>()
            .ShouldContain("offline.queue.dropped 4 >= 4");
    }

    [Fact]
    public async Task Null_queue_counters_do_not_trigger_threshold_policy()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewClientOptions(new ThrowingCounterStore()));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions
            {
                DegradedOfflineQueueDepthThreshold = 1,
                UnhealthyOfflineQueueDroppedCountThreshold = 1,
            });
        var result = await check.CheckHealthAsync(new HealthCheckContext(), timeout.Token);

        result.Status.ShouldBe(HealthStatus.Healthy);
        result.Data.ContainsKey("offline.queue.depth").ShouldBeFalse();
        result.Data.ContainsKey("offline.queue.dropped").ShouldBeFalse();
        result.Data.ContainsKey("health.policy.status").ShouldBeFalse();
    }

    [Fact]
    public async Task Policy_thresholds_do_not_improve_or_relabel_existing_unhealthy_states()
    {
        var store = new StaticMessageStore { Count = 10 };
        await using var client = new ResilientMqttClient(new LoopbackOnlyFactory(), NewClientOptions(store));
        var check = new PulseMqttHealthCheck(
            client,
            new PulseMqttHealthCheckOptions { UnhealthyOfflineQueueDepthThreshold = 1 });

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldBe("The client is Disconnected.");
        result.Data.ContainsKey("health.policy.status").ShouldBeFalse();
    }

    [Fact]
    public async Task Invalid_threshold_options_throw_clear_exceptions()
    {
        await using var client = new ResilientMqttClient(new LoopbackOnlyFactory(), NewClientOptions());

        var outOfRange = Should.Throw<ArgumentOutOfRangeException>(() =>
            new PulseMqttHealthCheck(
                client,
                new PulseMqttHealthCheckOptions { DegradedOfflineQueueDepthThreshold = 0 }));
        outOfRange.ParamName.ShouldBe(nameof(PulseMqttHealthCheckOptions.DegradedOfflineQueueDepthThreshold));

        var invalidOrder = Should.Throw<ArgumentException>(() =>
            new PulseMqttHealthCheck(
                client,
                new PulseMqttHealthCheckOptions
                {
                    DegradedPendingSubscriptionOperationsThreshold = 5,
                    UnhealthyPendingSubscriptionOperationsThreshold = 4,
                }));
        invalidOrder.ParamName.ShouldBe(nameof(PulseMqttHealthCheckOptions.DegradedPendingSubscriptionOperationsThreshold));

        var services = new ServiceCollection();
        var builder = services.AddPulseMqttClient("invalid-policy", ValidOptions);
        var builderFailure = Should.Throw<ArgumentOutOfRangeException>(() =>
            builder.AddHealthCheck(options => options.UnhealthyOfflineQueueDroppedCountThreshold = 0));
        builderFailure.ParamName.ShouldBe(nameof(PulseMqttHealthCheckOptions.UnhealthyOfflineQueueDroppedCountThreshold));
    }

    private static async Task WaitForStateAsync(
        ResilientMqttClient client,
        ConnectionState state,
        CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    private static async Task WaitForNotDisconnectedAsync(
        ResilientMqttClient client,
        CancellationToken cancellationToken)
    {
        while (client.State == ConnectionState.Disconnected)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    private sealed class StaticMessageStore : IMessageStore
    {
        public int Count { get; set; }

        public long DroppedCount { get; set; }

        public ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult<MqttPublishPacket?>(null);

        public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask ClearAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
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
}
