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

public sealed class MapMqttRequestTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task The_handler_return_value_is_the_reply()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var responder = NewClient(broker);
        await responder.ConnectAsync(timeout.Token);
        await responder.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        await using var endpoint = responder.MapMqttRequest<Reading, Reading>(
            "sensors/{deviceId:int}/calibrate",
            (reading, context) => ValueTask.FromResult(
                reading with { Value = reading.Value + context.Route.GetInt("deviceId") }));
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var requester = NewClient(broker);
        await requester.ConnectAsync(timeout.Token);
        await requester.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var reply = await requester.RequestAsync<Reading, Reading>(
            "sensors/7/calibrate", new Reading("dht22", 20.0), cancellationToken: timeout.Token);

        reply.ShouldBe(new Reading("dht22", 27.0));
    }

    [Fact]
    public async Task Host_mapped_request_handlers_resolve_scoped_services()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddPulseMqttClient("rpc", options =>
            {
                options.Host = "in-memory";
                options.ClientId = $"rpc-{Guid.NewGuid():N}"[..16];
            })
            .UseTransportFactory(_ => broker)
            .UseSerializer(_ => new JsonMqttSerializer(EndpointsJsonContext.Default));
        builder.Services.AddScoped<Offset>();
        using var app = builder.Build();
        await app.StartAsync(timeout.Token);
        var client = app.Services.GetRequiredService<IPulseMqttClientFactory>().GetClient("rpc");
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        await using var endpoint = app.MapMqttRequest<Reading, Reading>(
            "adjust/{amount:int}",
            (reading, context) =>
            {
                var offset = context.Services.GetRequiredService<Offset>();
                offset.Amount = context.Route.GetInt("amount");
                return ValueTask.FromResult(reading with { Value = reading.Value + offset.Amount });
            });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var requester = NewClient(broker);
        await requester.ConnectAsync(timeout.Token);
        await requester.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var reply = await requester.RequestAsync<Reading, Reading>(
            "adjust/5", new Reading("dht22", 1.0), cancellationToken: timeout.Token);

        reply.Value.ShouldBe(6.0);
        await app.StopAsync(timeout.Token);
    }

    private static ResilientMqttClient NewClient(PulseMqttTestBroker broker) =>
        new(broker, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = $"rq-{Guid.NewGuid():N}"[..16], KeepAliveSeconds = 0 },
            Serializer = new JsonMqttSerializer(EndpointsJsonContext.Default),
        });

    private sealed class Offset
    {
        public int Amount { get; set; }
    }
}
