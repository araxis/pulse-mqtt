using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.DependencyInjection.Tests;

public sealed class AddPulseMqttClientTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class LoopbackOnlyFactory : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
        {
            var (client, _) = LoopbackTransport.CreatePair();
            return ValueTask.FromResult(client);
        }
    }

    private static void ValidOptions(PulseMqttClientOptions options)
    {
        options.Host = "broker.local";
        options.ClientId = "test-client";
    }

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
            options.StartWithHost = false;
        }).UseTransportFactory(_ => new LoopbackOnlyFactory());
        await using var provider = services.BuildServiceProvider();

        var hosted = provider.GetRequiredService<IHostedService>();
        var client = provider.GetRequiredService<IPulseMqttClientFactory>().GetClient("manual");

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await hosted.StartAsync(timeout.Token);
        client.State.ShouldBe(ConnectionState.Disconnected); // the host did not start it

        // The application starts, stops, and restarts the client whenever it wants.
        await client.StartAsync(timeout.Token);
        while (client.State == ConnectionState.Disconnected)
        {
            await Task.Delay(5, timeout.Token);
        }

        await client.StopAsync(timeout.Token);
        client.State.ShouldBe(ConnectionState.Stopped);

        await client.StartAsync(timeout.Token);
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

        (await check.CheckHealthAsync(context)).Status.ShouldBe(HealthStatus.Unhealthy); // Disconnected

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        while (client.State == ConnectionState.Disconnected)
        {
            await Task.Delay(5, timeout.Token);
        }

        (await check.CheckHealthAsync(context)).Status.ShouldBe(HealthStatus.Degraded); // connecting/retrying

        await client.StopAsync(timeout.Token);
        (await check.CheckHealthAsync(context)).Status.ShouldBe(HealthStatus.Unhealthy); // Stopped
    }
}
