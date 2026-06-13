using System.Text;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

[Collection("mosquitto")]
public sealed class SessionRedeliveryIntegrationTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    private readonly MosquittoFixture _broker;

    public SessionRedeliveryIntegrationTests(MosquittoFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task A_qos2_publish_interrupted_by_a_drop_completes_after_the_real_broker_resumes()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        var topic = $"redeliver/{Guid.NewGuid():N}";
        var publisherId = $"publisher-{Guid.NewGuid():N}";

        // A durable subscriber on its own persistent session collects deliveries across the run.
        await using var subscriber = new RawMqttClient(NewTcpFactory());
        await subscriber.ConnectAsync(
            new MqttConnectPacket { ClientId = $"subscriber-{Guid.NewGuid():N}", KeepAliveSeconds = 30 },
            timeout.Token);
        await subscriber.SubscribeAsync(
            [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }],
            timeout.Token);

        // The publisher keeps a persistent session (CleanStart = false, non-zero expiry) so the
        // broker preserves its in-flight state across the reconnect.
        var killable = new KillableTransportFactory(NewTcpFactory());
        await using var publisher = new ResilientMqttClient(killable, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = publisherId,
                KeepAliveSeconds = 30,
                CleanStart = false,
                SessionExpiryInterval = 300,
            },
        });
        await publisher.StartAsync(timeout.Token);
        await WaitForConnectedAsync(publisher, timeout.Token);

        // Publish QoS 2 and cut the socket the instant the message is on the wire, so the
        // exchange is interrupted mid-flight.
        var publishTask = publisher.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = "exactly-once"u8.ToArray(), QualityOfService = MqttQualityOfService.ExactlyOnce },
            timeout.Token);

        await Task.Delay(50, timeout.Token);
        await killable.KillAsync();

        var outcome = await publishTask;
        outcome.Disposition.ShouldBeOneOf(PublishDisposition.InFlight, PublishDisposition.Delivered);

        // The resilient publisher reconnects to the preserved session and redelivers; the
        // subscriber receives the message exactly once.
        var received = await subscriber.Messages.ReadAsync(timeout.Token);
        received.Topic.ShouldBe(topic);
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("exactly-once");

        await publisher.StopAsync(timeout.Token);
        await subscriber.DisconnectAsync(timeout.Token);
    }

    private static async Task WaitForConnectedAsync(ResilientMqttClient client, CancellationToken cancellationToken)
    {
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    private TcpTransportFactory NewTcpFactory() =>
        new(new TcpTransportOptions { Host = _broker.Host, Port = _broker.Port });

    private sealed class KillableTransportFactory(IMqttTransportFactory inner) : IMqttTransportFactory
    {
        private volatile IMqttTransport? _current;

        public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
        {
            var transport = await inner.ConnectAsync(cancellationToken);
            _current = transport;
            return transport;
        }

        public ValueTask KillAsync() => _current?.DisposeAsync() ?? ValueTask.CompletedTask;
    }
}
