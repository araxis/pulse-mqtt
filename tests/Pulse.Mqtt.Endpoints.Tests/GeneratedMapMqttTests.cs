using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Endpoints.Tests;

/// <summary>
/// The Minimal-API-style handler shapes: these compile only because the source generator
/// intercepts each call site and lowers it onto the context runtime — the Delegate overload's
/// own body just throws. Every passing test here is proof the interceptor ran.
/// </summary>
public sealed class GeneratedMapMqttTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Route_payload_and_token_bind_in_any_order()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker, withSerializer: true);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var received = new TaskCompletionSource<(int Device, Reading Reading, bool HadToken)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt(
            "sensors/{deviceId:int}/reading",
            (Reading reading, int deviceId, CancellationToken ct) =>
            {
                received.TrySetResult((deviceId, reading, ct.CanBeCanceled));
                return ValueTask.CompletedTask;
            });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker, withSerializer: true);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync("sensors/31/reading", new Reading("bmp280", 1013.2), cancellationToken: timeout.Token);

        var (device, reading, hadToken) = await received.Task.WaitAsync(timeout.Token);
        device.ShouldBe(31);
        reading.ShouldBe(new Reading("bmp280", 1013.2));
        hadToken.ShouldBeTrue();
    }

    [Fact]
    public async Task Services_inject_into_host_mapped_handlers()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<Recorder>();
        builder.Services
            .AddPulseMqttClient("telemetry", options =>
            {
                options.Host = "in-memory";
                options.ClientId = $"gen-{Guid.NewGuid():N}"[..16];
            })
            .UseTransportFactory(_ => broker)
            .UseSerializer(_ => new JsonMqttSerializer(EndpointsJsonContext.Default));
        using var app = builder.Build();
        await app.StartAsync(timeout.Token);
        await app.Services.GetRequiredService<IPulseMqttClientFactory>()
            .GetClient("telemetry").WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        // The exact shape the feature exists for: route + payload + service + token, flat on app.
        await using var endpoint = app.MapMqtt(
            "sensors/{deviceId:int}/reading",
            (int deviceId, Reading reading, Recorder recorder, CancellationToken ct) =>
                recorder.SaveAsync(deviceId, reading, ct));
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker, withSerializer: true);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync("sensors/8/reading", new Reading("scd41", 612.0), cancellationToken: timeout.Token);

        var recorder = app.Services.GetRequiredService<Recorder>();
        var (device, reading) = await recorder.First.Task.WaitAsync(timeout.Token);
        device.ShouldBe(8);
        reading.ShouldBe(new Reading("scd41", 612.0));

        await app.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task Void_handlers_and_message_binding_work()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var received = new TaskCompletionSource<(string Topic, string Level)>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt(
            "alerts/{level}",
            (MqttPublishPacket message, string level) =>
            {
                received.TrySetResult((message.Topic, level));
            });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/red", Payload = "!"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);

        var (topic, level) = await received.Task.WaitAsync(timeout.Token);
        topic.ShouldBe("alerts/red");
        level.ShouldBe("red");
    }

    private static ResilientMqttClient NewClient(PulseMqttTestBroker broker, bool withSerializer = false) =>
        new(broker, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = $"gen-{Guid.NewGuid():N}"[..16], KeepAliveSeconds = 0 },
            Serializer = withSerializer ? new JsonMqttSerializer(EndpointsJsonContext.Default) : null,
        });

    public sealed class Recorder
    {
        public TaskCompletionSource<(int Device, Reading Reading)> First { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask SaveAsync(int device, Reading reading, CancellationToken cancellationToken)
        {
            First.TrySetResult((device, reading));
            return ValueTask.CompletedTask;
        }
    }
}
