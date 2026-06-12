using System.Threading.Channels;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

[Collection("mosquitto")]
public sealed class ServerDisconnectIntegrationTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    private readonly MosquittoFixture _broker;

    public ServerDisconnectIntegrationTests(MosquittoFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task A_session_takeover_surfaces_the_brokers_disconnect_reason()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        var clientId = $"takeover-{Guid.NewGuid():N}";

        await using var first = NewClient();
        await first.ConnectAsync(new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 }, timeout.Token);

        // A second connection with the same identifier makes the broker take the session over
        // and send the first client a DISCONNECT.
        await using var second = NewClient();
        await second.ConnectAsync(new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 }, timeout.Token);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await first.Messages.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttServerDisconnectedException>()
            .ReasonCode.ShouldBe(MqttReasonCode.SessionTakenOver);

        first.ServerDisconnect.ShouldNotBeNull();
        await second.DisconnectAsync(timeout.Token);
    }

    private RawMqttClient NewClient() =>
        new(new TcpTransportFactory(new TcpTransportOptions { Host = _broker.Host, Port = _broker.Port }));
}
