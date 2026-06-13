using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class TopicAliasTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    // ---- Outbound ----

    [Fact]
    public async Task Outbound_aliases_compress_repeated_topics_within_the_brokers_maximum()
    {
        var (client, broker, timeout) = await ConnectedAsync(
            useOutboundAliases: true, brokerAliasMaximum: 2, clientAliasMaximum: null);

        // First publish to a topic establishes the alias alongside the full topic.
        await Publish(client, "plant/line1/temp", timeout.Token);
        var first = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        first.Topic.ShouldBe("plant/line1/temp");
        first.TopicAlias.ShouldBe((ushort)1);

        // Repeats send only the alias.
        await Publish(client, "plant/line1/temp", timeout.Token);
        var repeat = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        repeat.Topic.ShouldBe(string.Empty);
        repeat.TopicAlias.ShouldBe((ushort)1);

        // A second topic takes the second alias.
        await Publish(client, "plant/line2/temp", timeout.Token);
        var second = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        second.Topic.ShouldBe("plant/line2/temp");
        second.TopicAlias.ShouldBe((ushort)2);

        // Beyond the maximum, topics go plain.
        await Publish(client, "plant/line3/temp", timeout.Token);
        var third = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        third.Topic.ShouldBe("plant/line3/temp");
        third.TopicAlias.ShouldBeNull();
    }

    [Fact]
    public async Task Outbound_aliases_stay_off_when_not_opted_in_or_not_offered()
    {
        var (optedOut, brokerA, timeoutA) = await ConnectedAsync(
            useOutboundAliases: false, brokerAliasMaximum: 10, clientAliasMaximum: null);
        await Publish(optedOut, "t", timeoutA.Token);
        (await brokerA.ReadPacketAsync(timeoutA.Token)).ShouldBeOfType<MqttPublishPacket>().TopicAlias.ShouldBeNull();

        var (noOffer, brokerB, timeoutB) = await ConnectedAsync(
            useOutboundAliases: true, brokerAliasMaximum: null, clientAliasMaximum: null);
        await Publish(noOffer, "t", timeoutB.Token);
        (await brokerB.ReadPacketAsync(timeoutB.Token)).ShouldBeOfType<MqttPublishPacket>().TopicAlias.ShouldBeNull();
    }

    // ---- Inbound ----

    [Fact]
    public async Task Inbound_aliases_resolve_to_the_established_topic()
    {
        var (client, broker, timeout) = await ConnectedAsync(
            useOutboundAliases: false, brokerAliasMaximum: null, clientAliasMaximum: 5);

        await broker.SendAsync(
            new MqttPublishPacket { Topic = "sensors/a", Payload = new byte[] { 1 }, TopicAlias = 3 },
            timeout.Token);
        (await client.Messages.ReadAsync(timeout.Token)).Topic.ShouldBe("sensors/a");

        await broker.SendAsync(
            new MqttPublishPacket { Topic = string.Empty, Payload = new byte[] { 2 }, TopicAlias = 3 },
            timeout.Token);
        var resolved = await client.Messages.ReadAsync(timeout.Token);
        resolved.Topic.ShouldBe("sensors/a");
        resolved.Payload.ToArray().ShouldBe(new byte[] { 2 });
    }

    [Fact]
    public async Task An_unestablished_inbound_alias_faults_the_session()
    {
        var (client, broker, timeout) = await ConnectedAsync(
            useOutboundAliases: false, brokerAliasMaximum: null, clientAliasMaximum: 5);

        await broker.SendAsync(
            new MqttPublishPacket { Topic = string.Empty, Payload = new byte[] { 1 }, TopicAlias = 2 },
            timeout.Token);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await client.Messages.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttProtocolException>().Message.ShouldContain("before it was established");
    }

    [Fact]
    public async Task An_out_of_range_inbound_alias_faults_the_session()
    {
        var (client, broker, timeout) = await ConnectedAsync(
            useOutboundAliases: false, brokerAliasMaximum: null, clientAliasMaximum: 2);

        await broker.SendAsync(
            new MqttPublishPacket { Topic = "t", Payload = new byte[] { 1 }, TopicAlias = 9 },
            timeout.Token);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await client.Messages.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttProtocolException>().Message.ShouldContain("outside the negotiated range");
    }

    [Fact]
    public async Task An_alias_without_negotiation_faults_the_session()
    {
        var (client, broker, timeout) = await ConnectedAsync(
            useOutboundAliases: false, brokerAliasMaximum: null, clientAliasMaximum: null);

        await broker.SendAsync(
            new MqttPublishPacket { Topic = "t", Payload = new byte[] { 1 }, TopicAlias = 1 },
            timeout.Token);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await client.Messages.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttProtocolException>().Message.ShouldContain("none were negotiated");
    }

    private static Task<MqttReasonCode> Publish(RawMqttClient client, string topic, CancellationToken cancellationToken) =>
        client.PublishAsync(new MqttPublishPacket { Topic = topic, Payload = new byte[] { 1 } }, cancellationToken);

    private static async Task<(RawMqttClient Client, ScriptedBroker Broker, CancellationTokenSource Timeout)> ConnectedAsync(
        bool useOutboundAliases,
        ushort? brokerAliasMaximum,
        ushort? clientAliasMaximum)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var client = new RawMqttClient(
            new FixedTransportFactory(clientTransport),
            new RawMqttClientOptions { UseOutboundTopicAliases = useOutboundAliases },
            new FakeTimeProvider());

        var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(
            new MqttConnectPacket { ClientId = "c", TopicAliasMaximum = clientAliasMaximum },
            timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { TopicAliasMaximum = brokerAliasMaximum }, timeout.Token);
        await connectTask;

        return (client, broker, timeout);
    }
}
