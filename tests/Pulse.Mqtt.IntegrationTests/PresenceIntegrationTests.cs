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
public sealed class PresenceIntegrationTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    private readonly MosquittoFixture _broker;

    public PresenceIntegrationTests(MosquittoFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task The_full_presence_cycle_works_without_application_code()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        var statusTopic = $"status/{Guid.NewGuid():N}";

        // An observer watches the retained status topic.
        await using var observer = new RawMqttClient(NewTcpFactory());
        await observer.ConnectAsync(new MqttConnectPacket { ClientId = $"observer-{Guid.NewGuid():N}" }, timeout.Token);
        await observer.SubscribeAsync(
            [new MqttTopicFilter(statusTopic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            timeout.Token);

        // The device announces "online" on every connection-up and leaves "offline" as its will.
        var killable = new KillableTransportFactory(NewTcpFactory());
        await using var device = new ResilientMqttClient(killable, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = $"device-{Guid.NewGuid():N}", KeepAliveSeconds = 5 },
            Birth = new MqttPublishPacket
            {
                Topic = statusTopic,
                Payload = "online"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                Retain = true,
            },
            Will = new MqttWillMessage(statusTopic)
            {
                Payload = "offline"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                Retain = true,
            },
        });
        await device.ConnectAsync(timeout.Token);

        // 1. Connect → birth → "online".
        (await NextStatusAsync(observer, timeout.Token)).ShouldBe("online");

        // 2. Ungraceful drop (no DISCONNECT packet) → the broker publishes the will → "offline".
        await killable.KillAsync();
        (await NextStatusAsync(observer, timeout.Token)).ShouldBe("offline");

        // 3. The automatic reconnect publishes the birth again → "online".
        (await NextStatusAsync(observer, timeout.Token)).ShouldBe("online");

        await device.DisconnectAsync(timeout.Token);
    }

    private static async Task<string> NextStatusAsync(RawMqttClient observer, CancellationToken cancellationToken)
    {
        var message = await observer.Messages.ReadAsync(cancellationToken);
        return Encoding.UTF8.GetString(message.Payload.Span);
    }

    private TcpTransportFactory NewTcpFactory() =>
        new(new TcpTransportOptions { Host = _broker.Host, Port = _broker.Port });

    /// <summary>Wraps a transport factory and lets the test cut the live connection abruptly.</summary>
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
