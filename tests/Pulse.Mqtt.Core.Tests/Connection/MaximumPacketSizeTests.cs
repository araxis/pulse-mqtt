using System.Buffers;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class MaximumPacketSizeTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    [Fact]
    public async Task A_publish_over_the_brokers_maximum_fails_before_the_wire()
    {
        var (client, broker, timeout) = await ConnectedAsync(maximumPacketSize: 32);

        var thrown = await Should.ThrowAsync<MqttPacketTooLargeException>(() => client.PublishAsync(
            new MqttPublishPacket { Topic = "a", Payload = new byte[64] },
            timeout.Token));

        thrown.Limit.ShouldBe(32u);
        thrown.PacketSize.ShouldBeGreaterThan(32);

        // The link is still healthy and nothing partial went out: a small publish flows.
        await client.PublishAsync(new MqttPublishPacket { Topic = "a", Payload = new byte[] { 1 } }, timeout.Token);
        var seen = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        seen.Payload.Length.ShouldBe(1);
    }

    [Fact]
    public async Task A_publish_exactly_at_the_maximum_is_sent()
    {
        var packet = new MqttPublishPacket { Topic = "a", Payload = new byte[16] };
        var scratch = new ArrayBufferWriter<byte>(64);
        MqttPacketWriter.Write(scratch, packet);
        var exactSize = (uint)scratch.WrittenCount;

        var (client, broker, timeout) = await ConnectedAsync(maximumPacketSize: exactSize);

        await client.PublishAsync(packet, timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>().Payload.Length.ShouldBe(16);
    }

    [Fact]
    public async Task No_advertised_maximum_means_no_client_side_limit()
    {
        var (client, broker, timeout) = await ConnectedAsync(maximumPacketSize: null);

        // Read concurrently: a packet this large exceeds the pipe's pause threshold, so the
        // flush only completes while the broker side is draining.
        var received = broker.ReadPacketAsync(timeout.Token);
        await client.PublishAsync(new MqttPublishPacket { Topic = "a", Payload = new byte[128 * 1024] }, timeout.Token);
        (await received).ShouldBeOfType<MqttPublishPacket>().Payload.Length.ShouldBe(128 * 1024);
    }

    [Fact]
    public async Task The_limit_applies_to_every_outbound_packet_type()
    {
        var (client, _, timeout) = await ConnectedAsync(maximumPacketSize: 16);

        var longFilter = new string('f', 64);
        await Should.ThrowAsync<MqttPacketTooLargeException>(() => client.SubscribeAsync(
            [new MqttTopicFilter(longFilter)],
            timeout.Token));
    }

    private static async Task<(RawMqttClient Client, ScriptedBroker Broker, CancellationTokenSource Timeout)> ConnectedAsync(
        uint? maximumPacketSize)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var client = new RawMqttClient(new FixedTransportFactory(clientTransport), timeProvider: new FakeTimeProvider());

        var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { MaximumPacketSize = maximumPacketSize }, timeout.Token);
        await connectTask;

        return (client, broker, timeout);
    }
}
