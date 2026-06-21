using System.Text.Json.Serialization;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Serialization.Json;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

internal sealed record TelemetryReading(string DeviceId, double Value);

[JsonSerializable(typeof(TelemetryReading))]
internal sealed partial class TestJsonContext : JsonSerializerContext;

public sealed class TypedMessagingTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private static JsonMqttSerializer NewSerializer() => new(TestJsonContext.Default);

    [Fact]
    public void Serializer_round_trips_and_stamps_json_metadata()
    {
        var serializer = NewSerializer();
        var reading = new TelemetryReading("d-1", 21.5);

        var payload = serializer.Serialize(reading);
        var roundTripped = serializer.Deserialize<TelemetryReading>(payload);

        serializer.ContentType.ShouldBe("application/json");
        serializer.PayloadFormat.ShouldBe(MqttPayloadFormatIndicator.Utf8);
        roundTripped.ShouldBe(reading);
    }

    [Fact]
    public async Task Typed_publish_carries_content_type_and_payload()
    {
        var (client, broker, ct) = await ConnectedAsync();
        await using var _ = client;

        await client.PublishAsync("telemetry/1", new TelemetryReading("d-1", 3.5), cancellationToken: ct);

        var seen = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        seen.Topic.ShouldBe("telemetry/1");
        seen.ContentType.ShouldBe("application/json");
        seen.PayloadFormatIndicator.ShouldBe(MqttPayloadFormatIndicator.Utf8);
        NewSerializer().Deserialize<TelemetryReading>(seen.Payload).ShouldBe(new TelemetryReading("d-1", 3.5));
    }

    [Fact]
    public async Task Typed_handler_receives_the_deserialized_value_and_route_parameters()
    {
        var (client, broker, ct) = await ConnectedAsync();
        await using var _ = client;

        var template = MqttRouteTemplate.Parse("telemetry/{device}");
        var subscribeTask = client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);

        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("telemetry/+");
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);
        await subscribeTask;

        var received = new TaskCompletionSource<(TelemetryReading Value, string Device)>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = client.RegisterRoute<TelemetryReading>(template, (value, message, _) =>
        {
            received.TrySetResult((value, message.Values["device"]));
            return ValueTask.CompletedTask;
        });

        await broker.SendAsync(new MqttPublishPacket
        {
            Topic = "telemetry/dev-9",
            Payload = NewSerializer().Serialize(new TelemetryReading("dev-9", 42.0)),
        }, ct);

        var (value, device) = await received.Task.WaitAsync(SafetyTimeout);
        value.ShouldBe(new TelemetryReading("dev-9", 42.0));
        device.ShouldBe("dev-9");
    }

    [Fact]
    public async Task Typed_messaging_requires_a_configured_serializer()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "untyped", KeepAliveSeconds = 0 },
        });

        Should.Throw<InvalidOperationException>(
            () => client.PublishAsync("t", new TelemetryReading("d", 1), cancellationToken: CancellationToken.None));
    }

    private static async Task<(ResilientMqttClient Client, TestBroker Broker, CancellationToken Ct)> ConnectedAsync()
    {
        var factory = new SequencedTransportFactory();
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "typed", KeepAliveSeconds = 0 },
            Serializer = NewSerializer(),
        });

        var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        // Publishing before the supervisor finishes connection-up would silently drop QoS 0.
        while (client.State != Pulse.Mqtt.Resilience.ConnectionState.Connected)
        {
            await Task.Delay(1, timeout.Token);
        }

        return (client, broker, timeout.Token);
    }
}
