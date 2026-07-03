using System.IO;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
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
    public async Task A_request_requires_mqtt_5()
    {
        var (client, _, _, ct) = await ConnectedAsync(MqttProtocolVersion.V311);
        await using var _1 = client;

        var thrown = await Should.ThrowAsync<NotSupportedException>(async () =>
            await client.RequestAsync<TelemetryReading, TelemetryReading>(
                "svc/echo",
                new TelemetryReading("d", 1.0),
                cancellationToken: ct));

        thrown.Message.ShouldContain("MQTT 5.0");
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

        var template = MqttRouteTemplate.Parse("svc/{op}");
        var subscribeTask = client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], ct);
        var subscribe = (await broker.ReadPacketAsync(ct)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("svc/+");
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, ct);
        await subscribeTask;

        using var responder = client.RegisterRequestHandler<TelemetryReading, TelemetryReading>(
            template,
            (request, message, _) =>
            {
                message.Values["op"].ShouldBe("double");
                return ValueTask.FromResult(request with { Value = request.Value * 2 });
            });

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
    public async Task A_responder_requires_mqtt_5()
    {
        await using var client = new ResilientMqttClient(new SequencedTransportFactory(), new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = "rpc",
                ProtocolVersion = MqttProtocolVersion.V311,
            },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
        });

        var thrown = Should.Throw<NotSupportedException>(() =>
            client.RegisterRequestHandler<TelemetryReading, TelemetryReading>(
                MqttRouteTemplate.Parse("svc/{op}"),
                (request, _, _) => ValueTask.FromResult(request)));

        thrown.Message.ShouldContain("MQTT 5.0");
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

    private static async Task<(ResilientMqttClient Client, TestBroker Broker, FakeTimeProvider Time, CancellationToken Ct)> ConnectedAsync(
        MqttProtocolVersion protocolVersion = MqttProtocolVersion.V500)
    {
        var factory = new SequencedTransportFactory();
        var time = new FakeTimeProvider();
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = "rpc",
                KeepAliveSeconds = 0,
                ProtocolVersion = protocolVersion,
            },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
        }, time);

        var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        while (client.State != Pulse.Mqtt.Resilience.ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        return (client, broker, time, timeout.Token);
    }

    [Fact]
    public async Task Rpc_recovers_after_the_first_response_route_setup_fails()
    {
        var factory = new SequencedTransportFactory();
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "rpc", KeepAliveSeconds = 0 },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
            SessionStore = new ThrowOnceOnUpsertSessionStore(),
        });
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        // The first request's response-route setup faults on the session store's first upsert.
        await Should.ThrowAsync<IOException>(async () =>
            await client.RequestAsync<TelemetryReading, TelemetryReading>(
                "svc/echo", new TelemetryReading("d", 1.0), cancellationToken: timeout.Token));

        // A second, healthy request must RETRY the route setup — not re-throw the cached failure.
        // Before the fix, _rpcRoute held the faulted task forever and this re-threw the IOException
        // without ever reaching the wire.
        var retry = client.RequestAsync<TelemetryReading, TelemetryReading>(
            "svc/echo", new TelemetryReading("d", 1.0), cancellationToken: timeout.Token);

        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("pulse-rpc/rpc/+");
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, timeout.Token);

        var request = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = Pulse.Mqtt.Codec.MqttPacketType.PubAck, PacketIdentifier = request.PacketIdentifier!.Value }, timeout.Token);
        await broker.SendAsync(new MqttPublishPacket
        {
            Topic = request.ResponseTopic!,
            Payload = new JsonMqttSerializer(TestJsonContext.Default).Serialize(new TelemetryReading("d", 2.0)),
            CorrelationData = request.CorrelationData,
        }, timeout.Token);

        (await retry).ShouldBe(new TelemetryReading("d", 2.0));
    }

    /// <summary>Throws once on the first UpsertSubscriptionsAsync (the RPC response-route setup), then works.</summary>
    private sealed class ThrowOnceOnUpsertSessionStore : ISessionStore
    {
        private readonly InMemorySessionStore _inner = new();
        private int _upserts;

        public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken) =>
            _inner.SaveSubscriptionsAsync(topicFilters, cancellationToken);

        public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken) =>
            _inner.LoadSubscriptionsAsync(cancellationToken);

        public ValueTask ClearAsync(CancellationToken cancellationToken) => _inner.ClearAsync(cancellationToken);

        public ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _upserts) == 1
                ? throw new IOException("transient session-store failure")
                : ((ISessionStore)_inner).UpsertSubscriptionsAsync(topicFilters, cancellationToken);
    }
}
