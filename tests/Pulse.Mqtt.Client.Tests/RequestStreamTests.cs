using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Serialization.Json;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class RequestStreamTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);
    private static readonly JsonMqttSerializer Serializer = new(TestJsonContext.Default);

    [Fact]
    public async Task A_request_stream_yields_responses_until_the_end_marker()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        await using var _1 = client;

        var received = new List<double>();
        var consume = Task.Run(async () =>
        {
            await foreach (var reading in client.RequestStreamAsync<TelemetryReading, TelemetryReading>(
                "svc/stream", new TelemetryReading("d", 0), cancellationToken: ct))
            {
                received.Add(reading.Value);
            }
        }, ct);

        var (responseTopic, correlation) = await DriveRequestAsync(broker, "svc/stream", ct);

        for (var i = 1; i <= 3; i++)
        {
            await broker.SendAsync(Response(responseTopic, correlation, i), ct);
        }

        await broker.SendAsync(EndMarker(responseTopic, correlation), ct);

        await consume.WaitAsync(SafetyTimeout);
        received.ShouldBe([1.0, 2.0, 3.0]);
    }

    [Fact]
    public async Task A_streaming_responder_publishes_each_item_then_the_end_marker()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        await using var _1 = client;

        var template = MqttRouteTemplate.Parse("svc/{op}");
        var subscribeTask = client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);
        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("svc/+");
        await broker.SendAsync(new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);
        await subscribeTask;

        using var responder = client.RegisterRequestStreamHandler<TelemetryReading, TelemetryReading>(
            template,
            (request, _, _) => Sequence(request, 2));

        await broker.SendAsync(new MqttPublishPacket
        {
            Topic = "svc/go",
            Payload = Serializer.Serialize(new TelemetryReading("d", 10)),
            ResponseTopic = "caller/inbox",
            CorrelationData = Encoding.UTF8.GetBytes("call-1"),
            QualityOfService = MqttQualityOfService.AtMostOnce,
        }, ct);

        var first = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        first.Topic.ShouldBe("caller/inbox");
        Serializer.Deserialize<TelemetryReading>(first.Payload).Value.ShouldBe(11.0);

        Serializer.Deserialize<TelemetryReading>((await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>().Payload).Value.ShouldBe(12.0);

        var marker = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        marker.Topic.ShouldBe("caller/inbox");
        marker.UserProperties.ShouldContain(p => p.Name == "pulse.eos");
        Encoding.UTF8.GetString(marker.CorrelationData!.Value.Span).ShouldBe("call-1");
    }

    [Fact]
    public async Task Abandoning_a_request_stream_early_stops_cleanly()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        await using var _1 = client;

        var firstReceived = new TaskCompletionSource<double>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consume = Task.Run(async () =>
        {
            await foreach (var reading in client.RequestStreamAsync<TelemetryReading, TelemetryReading>(
                "svc/stream", new TelemetryReading("d", 0), cancellationToken: ct))
            {
                firstReceived.TrySetResult(reading.Value);
                break; // abandon after the first response
            }
        }, ct);

        var (responseTopic, correlation) = await DriveRequestAsync(broker, "svc/stream", ct);
        await broker.SendAsync(Response(responseTopic, correlation, 1), ct);

        (await firstReceived.Task.WaitAsync(SafetyTimeout)).ShouldBe(1.0);
        await consume.WaitAsync(SafetyTimeout); // breaking disposes the enumerator and cleans up

        // Late responses to the abandoned stream are dropped gracefully; the client stays healthy.
        await broker.SendAsync(Response(responseTopic, correlation, 2), ct);
        await broker.SendAsync(EndMarker(responseTopic, correlation), ct);
        client.State.ShouldBe(ConnectionState.Connected);
    }

    [Fact]
    public async Task A_request_stream_times_out_when_no_response_arrives()
    {
        var (client, broker, time, ct) = await ConnectedAsync();
        await using var _1 = client;

        var consume = Task.Run(async () =>
        {
            await foreach (var _r in client.RequestStreamAsync<TelemetryReading, TelemetryReading>(
                "svc/stream",
                new TelemetryReading("d", 0),
                new MqttRequestStreamOptions { IdleTimeout = TimeSpan.FromSeconds(5) },
                ct))
            {
            }
        }, ct);

        await DriveRequestAsync(broker, "svc/stream", ct);

        for (var i = 0; i < 400 && !consume.IsCompleted; i++)
        {
            time.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(5);
        }

        var error = await Should.ThrowAsync<MqttException>(() => consume);
        error.Message.ShouldContain("stream response");
    }

    [Fact]
    public async Task A_consumer_that_falls_behind_capacity_fails_with_an_overflow_error()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        await using var _1 = client;

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? caught = null;
        var consume = Task.Run(async () =>
        {
            try
            {
                var seen = 0;
                await foreach (var _r in client.RequestStreamAsync<TelemetryReading, TelemetryReading>(
                    "svc/stream",
                    new TelemetryReading("d", 0),
                    new MqttRequestStreamOptions { Capacity = 1, IdleTimeout = TimeSpan.FromMinutes(5) },
                    ct))
                {
                    if (++seen == 1)
                    {
                        await release.Task; // stall after the first item so the buffer overflows
                    }
                }
            }
            catch (Exception ex)
            {
                caught = ex;
            }
        }, ct);

        var (responseTopic, correlation) = await DriveRequestAsync(broker, "svc/stream", ct);
        for (var i = 1; i <= 5; i++)
        {
            await broker.SendAsync(Response(responseTopic, correlation, i), ct);
        }

        await Task.Delay(100); // let the flood overflow the bounded buffer
        release.SetResult();

        await consume.WaitAsync(SafetyTimeout);
        caught.ShouldBeOfType<MqttException>().Message.ShouldContain("overflow");
    }

    private static async Task<(string ResponseTopic, ReadOnlyMemory<byte> Correlation)> DriveRequestAsync(TestBroker broker, string topic, CancellationToken ct)
    {
        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("pulse-rpc/rpc/+");
        await broker.SendAsync(new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);

        var request = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        request.Topic.ShouldBe(topic);
        request.ResponseTopic.ShouldNotBeNull();
        request.CorrelationData.ShouldNotBeNull();
        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = request.PacketIdentifier!.Value }, ct);

        return (request.ResponseTopic!, request.CorrelationData!.Value);
    }

    private static MqttPublishPacket Response(string topic, ReadOnlyMemory<byte> correlation, double value) => new()
    {
        Topic = topic,
        Payload = Serializer.Serialize(new TelemetryReading("d", value)),
        CorrelationData = correlation,
    };

    private static MqttPublishPacket EndMarker(string topic, ReadOnlyMemory<byte> correlation) => new()
    {
        Topic = topic,
        CorrelationData = correlation,
        UserProperties = [new MqttUserProperty("pulse.eos", "true")],
    };

    private static async IAsyncEnumerable<TelemetryReading> Sequence(TelemetryReading seed, int count)
    {
        for (var i = 1; i <= count; i++)
        {
            await Task.Yield();
            yield return seed with { Value = seed.Value + i };
        }
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
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        return (client, broker, time, timeout.Token);
    }
}
