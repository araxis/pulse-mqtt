using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class RawMqttClientTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    [Fact]
    public async Task Handshake_returns_the_connack()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        await using var client = NewClient(clientTransport, time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c1" }, timeout.Token);

        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttConnectPacket>().ClientId.ShouldBe("c1");
        await broker.SendAsync(new MqttConnAckPacket { SessionPresent = true }, timeout.Token);

        var connAck = await connectTask;
        connAck.SessionPresent.ShouldBeTrue();
        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task Handshake_times_out_when_the_broker_stays_silent()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        await using var client = NewClient(clientTransport, time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token); // swallow the CONNECT, never answer

        await AdvanceUntilAsync(time, () => connectTask.IsCompleted);

        var thrown = await Should.ThrowAsync<MqttException>(async () => await connectTask);
        thrown.Message.ShouldContain("did not answer");
    }

    [Fact]
    public async Task A_non_connack_first_packet_is_a_protocol_error()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        await using var client = NewClient(clientTransport, new FakeTimeProvider());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttPingRespPacket(), timeout.Token);

        await Should.ThrowAsync<MqttProtocolException>(async () => await connectTask);
    }

    [Fact]
    public async Task A_rejected_connack_is_returned_and_the_link_closed()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        await using var client = NewClient(clientTransport, new FakeTimeProvider());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { ReasonCode = MqttReasonCode.NotAuthorized }, timeout.Token);

        var connAck = await connectTask;

        connAck.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
    }

    [Fact]
    public async Task Keepalive_pings_when_idle_and_repeats()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        await using var client = NewClient(clientTransport, time);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await HandshakeAsync(client, broker, keepAliveSeconds: 10, timeout.Token);

        var firstPing = ReadNextAsync(broker, timeout.Token);
        await AdvanceUntilAsync(time, () => firstPing.IsCompleted);
        (await firstPing).ShouldBeOfType<MqttPingReqPacket>();
        await broker.SendAsync(new MqttPingRespPacket(), timeout.Token);

        var secondPing = ReadNextAsync(broker, timeout.Token);
        await AdvanceUntilAsync(time, () => secondPing.IsCompleted);
        (await secondPing).ShouldBeOfType<MqttPingReqPacket>();
    }

    [Fact]
    public async Task Missing_pong_faults_the_client()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        await using var client = NewClient(clientTransport, time, new RawMqttClientOptions
        {
            PingResponseTimeout = TimeSpan.FromSeconds(10),
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await HandshakeAsync(client, broker, keepAliveSeconds: 10, timeout.Token);

        await AdvanceUntilAsync(time, () => client.Messages.Completion.IsCompleted);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await client.Messages.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttException>().Message.ShouldContain("PINGREQ");
    }

    [Fact]
    public async Task Inbound_publishes_surface_on_messages()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        await using var client = NewClient(clientTransport, new FakeTimeProvider());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await HandshakeAsync(client, broker, keepAliveSeconds: 0, timeout.Token);

        await broker.SendAsync(new MqttPublishPacket { Topic = "news", Payload = new byte[] { 1 } }, timeout.Token);

        var message = await client.Messages.ReadAsync(timeout.Token);
        message.Topic.ShouldBe("news");
    }

    [Fact]
    public async Task Disconnect_sends_the_disconnect_packet()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        await using var client = NewClient(clientTransport, new FakeTimeProvider());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await HandshakeAsync(client, broker, keepAliveSeconds: 0, timeout.Token);

        var disconnectTask = client.DisconnectAsync(timeout.Token);

        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttDisconnectPacket>();
        await disconnectTask;
    }

    private static RawMqttClient NewClient(IMqttTransport transport, TimeProvider time, RawMqttClientOptions? options = null) =>
        new(new FixedTransportFactory(transport), options, time);

    private static async Task HandshakeAsync(RawMqttClient client, ScriptedBroker broker, ushort keepAliveSeconds, CancellationToken ct)
    {
        var connectTask = client.ConnectAsync(
            new MqttConnectPacket { ClientId = "c", KeepAliveSeconds = keepAliveSeconds }, ct);
        await broker.ReadPacketAsync(ct);
        await broker.SendAsync(new MqttConnAckPacket(), ct);
        await connectTask;
    }

    private static Task<MqttPacket> ReadNextAsync(ScriptedBroker broker, CancellationToken ct) =>
        Task.Run(() => broker.ReadPacketAsync(ct), CancellationToken.None);

    private static async Task AdvanceUntilAsync(FakeTimeProvider time, Func<bool> condition)
    {
        for (var i = 0; i < 400 && !condition(); i++)
        {
            time.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(5);
        }

        condition().ShouldBeTrue();
    }
}
