using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class ServerDisconnectTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    [Fact]
    public async Task A_broker_disconnect_fails_pending_publishes_with_the_reason()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        await using var client = new RawMqttClient(new FixedTransportFactory(clientTransport), timeProvider: new FakeTimeProvider());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "a", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();

        await broker.SendAsync(
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.ServerShuttingDown,
                ReasonString = "maintenance window",
                ServerReference = "backup.example:1883",
            },
            timeout.Token);

        var thrown = await Should.ThrowAsync<MqttServerDisconnectedException>(async () => await publishTask);
        thrown.ReasonCode.ShouldBe(MqttReasonCode.ServerShuttingDown);
        thrown.ReasonString.ShouldBe("maintenance window");
        thrown.ServerReference.ShouldBe("backup.example:1883");

        client.ServerDisconnect.ShouldNotBeNull().ReasonCode.ShouldBe(MqttReasonCode.ServerShuttingDown);
    }

    [Fact]
    public async Task A_broker_disconnect_completes_the_message_stream_with_the_reason()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        await using var client = new RawMqttClient(new FixedTransportFactory(clientTransport), timeProvider: new FakeTimeProvider());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        await broker.SendAsync(new MqttDisconnectPacket { ReasonCode = MqttReasonCode.SessionTakenOver }, timeout.Token);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await client.Messages.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttServerDisconnectedException>()
            .ReasonCode.ShouldBe(MqttReasonCode.SessionTakenOver);
    }
}
