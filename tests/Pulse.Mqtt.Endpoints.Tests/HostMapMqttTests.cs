using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Endpoints.Tests;

public sealed class HostMapMqttTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_single_client_app_maps_without_naming_the_client()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        using var app = NewApp(broker, "telemetry");
        await app.StartAsync(timeout.Token);
        await WaitConnectedAsync(app, "telemetry", timeout.Token);

        var received = new TaskCompletionSource<(int Device, string Text, bool ScopedServices)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = app.MapMqtt("sensors/{deviceId:int}/temp", context =>
        {
            received.TrySetResult((
                context.Route.GetInt("deviceId"),
                Encoding.UTF8.GetString(context.Message.Payload.Span),
                context.HasServices && context.Services.GetService<ScopedProbe>() is not null));
            return ValueTask.CompletedTask;
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await PublishAsync(broker, "sensors/12/temp", "19.5", timeout.Token);

        var (device, text, scopedServices) = await received.Task.WaitAsync(timeout.Token);
        device.ShouldBe(12);
        text.ShouldBe("19.5");
        scopedServices.ShouldBeTrue(); // the host's services flow in, scoped per message

        await app.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task A_multi_client_app_requires_the_client_name()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddScoped<ScopedProbe>();
        builder.Services.AddPulseMqttClient("north", options => Configure(options)).UseTransportFactory(_ => broker);
        builder.Services.AddPulseMqttClient("south", options => Configure(options)).UseTransportFactory(_ => broker);
        using var app = builder.Build();
        await app.StartAsync(timeout.Token);
        await WaitConnectedAsync(app, "south", timeout.Token);

        var nameless = Should.Throw<InvalidOperationException>(
            () => app.MapMqtt("a/{b}", _ => ValueTask.CompletedTask));
        nameless.Message.ShouldContain("'north'");
        nameless.Message.ShouldContain("'south'");

        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = app.MapMqtt("south", "plants/{plant}", context =>
        {
            received.TrySetResult(context.Route.GetString("plant"));
            return ValueTask.CompletedTask;
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await PublishAsync(broker, "plants/west", "x", timeout.Token);
        (await received.Task.WaitAsync(timeout.Token)).ShouldBe("west");

        await app.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task An_app_without_clients_explains_what_to_register()
    {
        using var app = Host.CreateApplicationBuilder().Build();
        var error = Should.Throw<InvalidOperationException>(
            () => app.MapMqtt("a/{b}", _ => ValueTask.CompletedTask));
        error.Message.ShouldContain("AddPulseMqttClient");
        await Task.CompletedTask;
    }

    private static Task WaitConnectedAsync(IHost app, string name, CancellationToken token) =>
        app.Services.GetRequiredService<IPulseMqttClientFactory>()
            .GetClient(name)
            .WaitUntilConnectedAsync(SafetyTimeout, token);

    private static IHost NewApp(PulseMqttTestBroker broker, string name)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddScoped<ScopedProbe>();
        builder.Services.AddPulseMqttClient(name, options => Configure(options)).UseTransportFactory(_ => broker);
        return builder.Build();
    }

    private static void Configure(PulseMqttClientOptions options)
    {
        options.Host = "in-memory";
        options.ClientId = $"host-{Guid.NewGuid():N}"[..16];
    }

    private static async Task PublishAsync(PulseMqttTestBroker broker, string topic, string text, CancellationToken token)
    {
        await using var publisher = new Pulse.Mqtt.Connection.RawMqttClient(broker);
        await publisher.ConnectAsync(
            new MqttConnectPacket { ClientId = $"pub-{Guid.NewGuid():N}"[..16], KeepAliveSeconds = 0 }, token);
        var result = await publisher.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = Encoding.UTF8.GetBytes(text), QualityOfService = MqttQualityOfService.AtLeastOnce },
            token);
        await publisher.DisconnectAsync(token);
    }

    private sealed class ScopedProbe;
}
