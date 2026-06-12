using System.Text;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

[Collection("mosquitto")]
public sealed class RawMqttClientIntegrationTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    private readonly MosquittoFixture _broker;

    public RawMqttClientIntegrationTests(MosquittoFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task V5_handshake_succeeds()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        await using var client = NewClient();

        var connAck = await client.ConnectAsync(NewConnect(MqttProtocolVersion.V500), timeout.Token);

        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        connAck.SessionPresent.ShouldBeFalse();
        await client.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task V311_handshake_succeeds()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        await using var client = NewClient();

        var connAck = await client.ConnectAsync(NewConnect(MqttProtocolVersion.V311), timeout.Token);

        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        await client.DisconnectAsync(timeout.Token);
    }

    [Theory]
    [InlineData(MqttQualityOfService.AtMostOnce)]
    [InlineData(MqttQualityOfService.AtLeastOnce)]
    [InlineData(MqttQualityOfService.ExactlyOnce)]
    public async Task Publish_roundtrips_through_the_broker(MqttQualityOfService qos)
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        await using var client = NewClient();
        await client.ConnectAsync(NewConnect(MqttProtocolVersion.V500), timeout.Token);

        var topic = $"pulse/it/{Guid.NewGuid():N}";
        var granted = await client.SubscribeAsync(
            [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }],
            timeout.Token);
        granted.ShouldHaveSingleItem().ShouldBe(MqttReasonCode.GrantedQualityOfService2);

        var payload = Encoding.UTF8.GetBytes($"hello-{qos}");
        var result = await client.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = payload, QualityOfService = qos },
            timeout.Token);
        result.ShouldBe(MqttReasonCode.Success);

        var received = await client.Messages.ReadAsync(timeout.Token);
        received.Topic.ShouldBe(topic);
        received.QualityOfService.ShouldBe(qos);
        received.Payload.ToArray().ShouldBe(payload);

        await client.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task A_message_flows_between_two_clients()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        await using var subscriber = NewClient();
        await using var publisher = NewClient();
        await subscriber.ConnectAsync(NewConnect(MqttProtocolVersion.V500), timeout.Token);
        await publisher.ConnectAsync(NewConnect(MqttProtocolVersion.V500), timeout.Token);

        var topic = $"pulse/it/{Guid.NewGuid():N}";
        await subscriber.SubscribeAsync([new MqttTopicFilter(topic)], timeout.Token);

        await publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = topic,
                Payload = "cross"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);

        var received = await subscriber.Messages.ReadAsync(timeout.Token);
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("cross");

        await publisher.DisconnectAsync(timeout.Token);
        await subscriber.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task Unsubscribe_stops_delivery()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        await using var client = NewClient();
        await client.ConnectAsync(NewConnect(MqttProtocolVersion.V500), timeout.Token);

        var topic = $"pulse/it/{Guid.NewGuid():N}";
        await client.SubscribeAsync([new MqttTopicFilter(topic)], timeout.Token);
        var codes = await client.UnsubscribeAsync([topic], timeout.Token);
        codes.ShouldHaveSingleItem().ShouldBe(MqttReasonCode.Success);

        await client.PublishAsync(new MqttPublishPacket { Topic = topic, Payload = "x"u8.ToArray() }, timeout.Token);

        using var shortWait = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await Should.ThrowAsync<OperationCanceledException>(
            async () => await client.Messages.ReadAsync(shortWait.Token));

        await client.DisconnectAsync(timeout.Token);
    }

    private RawMqttClient NewClient() =>
        new(new TcpTransportFactory(new TcpTransportOptions { Host = _broker.Host, Port = _broker.Port }));

    private static MqttConnectPacket NewConnect(MqttProtocolVersion version) => new()
    {
        ClientId = $"pulse-it-{Guid.NewGuid():N}"[..20],
        ProtocolVersion = version,
        CleanStart = true,
        KeepAliveSeconds = 30,
    };
}
