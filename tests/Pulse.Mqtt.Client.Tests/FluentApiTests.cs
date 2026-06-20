using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class FluentApiTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task The_builder_creates_a_working_client_over_a_custom_transport()
    {
        await using var broker = new PulseMqttTestBroker();
        using var timeout = new CancellationTokenSource(SafetyTimeout);

        await using var client = await new PulseMqttClientBuilder()
            .WithTransport(broker)
            .WithClientId("fluent")
            .WithoutKeepAlive()
            .WithSerializer(new JsonMqttSerializer(TestJsonContext.Default))
            .WithBackoff(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(100))
            .WithOfflineQueue(capacity: 16, OverflowPolicy.DropOldest)
            .BuildAndStartAsync(timeout.Token);

        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        client.State.ShouldBe(ConnectionState.Connected);
    }

    [Fact]
    public void The_builder_requires_a_transport()
    {
        var thrown = Should.Throw<InvalidOperationException>(() => new PulseMqttClientBuilder().WithClientId("x").Build());
        thrown.Message.ShouldContain("transport");
    }

    [Fact]
    public void The_builder_rejects_mixing_a_full_connect_packet_with_identity_methods()
    {
        var builder = new PulseMqttClientBuilder()
            .WithTcp("broker.local")
            .WithConnect(new MqttConnectPacket { ClientId = "a" })
            .WithClientId("b");

        Should.Throw<InvalidOperationException>(builder.Build);
    }

    [Fact]
    public async Task A_fluent_publish_carries_qos_retain_and_properties()
    {
        var (client, broker, ct) = await ConnectedAsync();
        await using var _ = client;

        var outcome = await client.Publish("plant/line1/state")
            .AtLeastOnce()
            .WithRetain()
            .WithMessageExpiry(TimeSpan.FromMinutes(5))
            .WithUserProperty("tenant", "acme")
            .WithPayload(new TelemetryReading("d-1", 7.5))
            .SendAsync(ct);

        outcome.Disposition.ShouldBe(PublishDisposition.Delivered);

        var seen = await broker.ClientPublishes.ReadAsync(ct);
        seen.Topic.ShouldBe("plant/line1/state");
        seen.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        seen.Retain.ShouldBeTrue();
        seen.MessageExpiryInterval.ShouldBe(300u);
        seen.ContentType.ShouldBe("application/json");
        seen.UserProperties.ShouldHaveSingleItem().ShouldBe(new MqttUserProperty("tenant", "acme"));
        new JsonMqttSerializer(TestJsonContext.Default)
            .Deserialize<TelemetryReading>(seen.Payload).ShouldBe(new TelemetryReading("d-1", 7.5));
    }

    [Fact]
    public async Task A_fluent_route_receives_typed_messages_with_captured_values()
    {
        var (client, broker, ct) = await ConnectedAsync();
        await using var _ = client;

        var received = new TaskCompletionSource<(TelemetryReading Value, string Device)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var route = client.Route("sensors/{device}/telemetry");
        await client.SubscribeAsync([route.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);

        using var registration = route
            .WithQueue(capacity: 8, RouteOverflow.DropOldest)
            .WithConcurrency(2)
            .Handle<TelemetryReading>((value, message, _) =>
            {
                received.TrySetResult((value, message.Values["device"]));
                return ValueTask.CompletedTask;
            });

        await broker.PublishAsync(new MqttPublishPacket
        {
            Topic = "sensors/boiler-9/telemetry",
            Payload = new JsonMqttSerializer(TestJsonContext.Default).Serialize(new TelemetryReading("d-9", 42.0)),
        }, ct);

        var (value, device) = await received.Task.WaitAsync(SafetyTimeout);
        value.ShouldBe(new TelemetryReading("d-9", 42.0));
        device.ShouldBe("boiler-9");
    }

    [Fact]
    public async Task A_fluent_request_round_trips_through_a_responder()
    {
        var (client, _, ct) = await ConnectedAsync();
        await using var _1 = client;

        var template = MqttRouteTemplate.Parse("calibrate/{device}");
        await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);
        using var responder = client.RegisterRequestHandler<TelemetryReading, TelemetryReading>(
            template,
            (request, message, _) =>
                ValueTask.FromResult(request with { Value = request.Value * 2 }));

        var reply = await client.Request("calibrate/boiler-1")
            .WithTimeout(TimeSpan.FromSeconds(5))
            .SendAsync<TelemetryReading, TelemetryReading>(new TelemetryReading("d-1", 10), ct);

        reply.ShouldBe(new TelemetryReading("d-1", 20));
    }

    private static async Task<(ResilientMqttClient Client, PulseMqttTestBroker Broker, CancellationToken Ct)> ConnectedAsync()
    {
        var broker = new PulseMqttTestBroker();
        var timeout = new CancellationTokenSource(SafetyTimeout);

        var client = await new PulseMqttClientBuilder()
            .WithTransport(broker)
            .WithClientId("fluent")
            .WithoutKeepAlive()
            .WithSerializer(new JsonMqttSerializer(TestJsonContext.Default))
            .BuildAndStartAsync(timeout.Token);

        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        return (client, broker, timeout.Token);
    }
}
