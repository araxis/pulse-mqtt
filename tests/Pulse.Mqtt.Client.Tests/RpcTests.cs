using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Serialization.Json;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class RpcTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_typed_request_round_trips_with_correlation()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        await using var _1 = client;

        var requestTask = client.RequestAsync<TelemetryReading, TelemetryReading>(
            "svc/echo", new TelemetryReading("d", 1.0), cancellationToken: ct);

        // First call lazily subscribes the response route.
        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("pulse-rpc/rpc/+");
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);

        var request = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        request.Topic.ShouldBe("svc/echo");
        request.ResponseTopic.ShouldNotBeNull();
        request.ResponseTopic!.ShouldStartWith("pulse-rpc/rpc/");
        request.CorrelationData.ShouldNotBeNull();
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = Pulse.Mqtt.Codec.MqttPacketType.PubAck, PacketIdentifier = request.PacketIdentifier!.Value }, ct);

        await broker.SendAsync(new MqttPublishPacket
        {
            Topic = request.ResponseTopic!,
            Payload = new JsonMqttSerializer(TestJsonContext.Default).Serialize(new TelemetryReading("d", 2.0)),
            CorrelationData = request.CorrelationData,
        }, ct);

        (await requestTask).ShouldBe(new TelemetryReading("d", 2.0));
    }

    [Fact]
    public async Task A_request_times_out_without_a_response()
    {
        var (client, broker, time, ct) = await ConnectedAsync();
        await using var _1 = client;

        var requestTask = client.RequestAsync<TelemetryReading, TelemetryReading>(
            "svc/slow", new TelemetryReading("d", 1.0), cancellationToken: ct);

        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);
        var request = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = Pulse.Mqtt.Codec.MqttPacketType.PubAck, PacketIdentifier = request.PacketIdentifier!.Value }, ct);

        for (var i = 0; i < 400 && !requestTask.IsCompleted; i++)
        {
            time.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(5);
        }

        var thrown = await Should.ThrowAsync<MqttException>(async () => await requestTask);
        thrown.Message.ShouldContain("No response");
    }

    [Fact]
    public async Task A_responder_answers_to_the_response_topic_with_the_correlation_echoed()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        await using var _1 = client;
        var serializer = new JsonMqttSerializer(TestJsonContext.Default);

        var onTask = client.OnRequestAsync<TelemetryReading, TelemetryReading>(
            "svc/{op}",
            (request, message, _) =>
            {
                message.Values["op"].ShouldBe("double");
                return ValueTask.FromResult(request with { Value = request.Value * 2 });
            },
            cancellationToken: ct);
        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("svc/+");
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);
        await onTask;

        await broker.SendAsync(new MqttPublishPacket
        {
            Topic = "svc/double",
            Payload = serializer.Serialize(new TelemetryReading("d", 21.0)),
            ResponseTopic = "caller/inbox",
            CorrelationData = Encoding.UTF8.GetBytes("call-1"),
        }, ct);

        var reply = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        reply.Topic.ShouldBe("caller/inbox");
        Encoding.UTF8.GetString(reply.CorrelationData!.Value.Span).ShouldBe("call-1");
        serializer.Deserialize<TelemetryReading>(reply.Payload).Value.ShouldBe(42.0);
    }

    [Fact]
    public async Task An_offline_request_fails_fast()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "rpc", KeepAliveSeconds = 0 },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
        });

        var thrown = await Should.ThrowAsync<MqttException>(async () =>
            await client.RequestAsync<TelemetryReading, TelemetryReading>(
                "svc/x", new TelemetryReading("d", 1.0), cancellationToken: CancellationToken.None));
        thrown.Message.ShouldContain("offline");
    }

    private static async Task<(ResilientMqttClient Client, TestBroker Broker, FakeTimeProvider Time, CancellationToken Ct)> ConnectedAsync()
    {
        var factory = new SequencedTransportFactory();
        var time = new FakeTimeProvider();
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "rpc", KeepAliveSeconds = 0 },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
        }, time);

        var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        while (client.State != Pulse.Mqtt.Resilience.ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        return (client, broker, time, timeout.Token);
    }
}
