using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class MaximumPacketSizeTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_too_large_publish_fails_the_caller_instead_of_queueing()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token, maximumPacketSize: 32);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await Should.ThrowAsync<MqttPacketTooLargeException>(() => client.PublishAsync(
            new MqttPublishPacket { Topic = "a", Payload = new byte[64], QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token));
    }

    [Fact]
    public async Task A_queued_publish_too_large_for_the_new_session_is_dropped_and_the_queue_keeps_draining()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // Kill the session, queue two publishes while offline: one the next broker will
        // refuse by size, one it will accept.
        await broker1.DisposeAsync();
        await WaitForStateAsync(client, ConnectionState.Reconnecting, timeout.Token);

        (await client.PublishAsync(
            new MqttPublishPacket { Topic = "big", Payload = new byte[64], QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token)).Disposition.ShouldBe(PublishDisposition.Queued);
        (await client.PublishAsync(
            new MqttPublishPacket { Topic = "small", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token)).Disposition.ShouldBe(PublishDisposition.Queued);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, maximumPacketSize: 32);

        // The flush drops the oversized message and still delivers the small one.
        var flushed = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        flushed.Topic.ShouldBe("small");
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = flushed.PacketIdentifier!.Value },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    private static ResilientMqttClient NewClient(SequencedTransportFactory factory) =>
        new(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "max-size", KeepAliveSeconds = 0 },
        });

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
