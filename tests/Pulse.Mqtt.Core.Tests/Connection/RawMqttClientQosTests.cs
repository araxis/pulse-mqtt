using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class RawMqttClientQosTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    [Fact]
    public async Task Qos0_publish_carries_no_packet_identifier()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var result = await client.PublishAsync(new MqttPublishPacket { Topic = "t" }, ct);

        result.ShouldBe(MqttReasonCode.Success);
        var seen = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>();
        seen.PacketIdentifier.ShouldBeNull();
    }

    [Fact]
    public async Task Qos1_publish_completes_on_puback()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);

        var seen = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>();
        var id = seen.PacketIdentifier!.Value;
        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = id }, ct);

        (await publishTask).ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task Qos1_publish_times_out_without_puback()
    {
        var (client, broker, time, ct) = await ConnectedAsync();

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);
        await broker.ReadPacketAsync(ct); // swallow the PUBLISH, never acknowledge

        await AdvanceUntilAsync(time, () => publishTask.IsCompleted);

        var thrown = await Should.ThrowAsync<MqttException>(async () => await publishTask);
        thrown.Message.ShouldContain("did not acknowledge");
    }

    [Fact]
    public async Task Qos2_publish_walks_the_full_handshake()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.ExactlyOnce }, ct);

        var seen = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>();
        var id = seen.PacketIdentifier!.Value;
        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubRec, PacketIdentifier = id }, ct);

        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>().PacketType.ShouldBe(MqttPacketType.PubRel);
        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubComp, PacketIdentifier = id }, ct);

        (await publishTask).ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task Concurrent_publishes_use_distinct_identifiers()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var first = client.PublishAsync(new MqttPublishPacket { Topic = "a", QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);
        var second = client.PublishAsync(new MqttPublishPacket { Topic = "b", QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);

        var seenA = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>();
        var seenB = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>();
        seenA.PacketIdentifier.ShouldNotBe(seenB.PacketIdentifier);

        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seenA.PacketIdentifier!.Value }, ct);
        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seenB.PacketIdentifier!.Value }, ct);

        (await first).ShouldBe(MqttReasonCode.Success);
        (await second).ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task Inbound_qos1_publish_is_delivered_then_acked()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        await broker.SendAsync(
            new MqttPublishPacket { Topic = "n", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 9 }, ct);

        (await client.Messages.ReadAsync(ct)).Topic.ShouldBe("n");
        var ack = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>();
        ack.PacketType.ShouldBe(MqttPacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)9);
    }

    [Fact]
    public async Task Acknowledged_inbound_qos1_publish_waits_for_consumer_ack()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        var received = new TaskCompletionSource<MqttInboundPublishContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.AcknowledgedMessageSink = (context, _) =>
        {
            received.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await broker.SendAsync(
            new MqttPublishPacket { Topic = "n", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 9 }, ct);

        var context = await received.Task.WaitAsync(SafetyTimeout);
        context.Message.Topic.ShouldBe("n");

        var ackRead = Task.Run(() => broker.ReadPacketAsync(ct), CancellationToken.None);
        (await Task.WhenAny(ackRead, Task.Delay(100, ct))).ShouldNotBe(ackRead);

        await context.AcknowledgeAsync(ct);

        var ack = (await ackRead.WaitAsync(SafetyTimeout)).ShouldBeOfType<MqttPublishAckPacket>();
        ack.PacketType.ShouldBe(MqttPacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)9);
    }

    [Fact]
    public async Task Acknowledged_inbound_qos1_publish_can_be_rejected()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        var received = new TaskCompletionSource<MqttInboundPublishContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.AcknowledgedMessageSink = (context, _) =>
        {
            received.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await broker.SendAsync(
            new MqttPublishPacket { Topic = "n", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 10 }, ct);

        var context = await received.Task.WaitAsync(SafetyTimeout);
        context.CanReject.ShouldBeTrue();
        await context.RejectAsync(MqttReasonCode.UnspecifiedError, "failed", ct);

        var ack = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>();
        ack.PacketType.ShouldBe(MqttPacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)10);
        ack.ReasonCode.ShouldBe(MqttReasonCode.UnspecifiedError);
        ack.ReasonString.ShouldBe("failed");
    }

    [Fact]
    public async Task Acknowledged_inbound_qos1_publish_cannot_be_rejected_on_mqtt_3_1_1()
    {
        var (client, broker, _, ct) = await ConnectedAsync(MqttProtocolVersion.V311);
        var received = new TaskCompletionSource<MqttInboundPublishContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.AcknowledgedMessageSink = (context, _) =>
        {
            received.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await broker.SendAsync(
            new MqttPublishPacket
            {
                Topic = "n",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                PacketIdentifier = 11,
                ProtocolVersion = MqttProtocolVersion.V311,
            },
            ct);

        var context = await received.Task.WaitAsync(SafetyTimeout);
        context.CanReject.ShouldBeFalse();

        await Should.ThrowAsync<NotSupportedException>(
            async () => await context.RejectAsync(MqttReasonCode.UnspecifiedError, "failed", ct));
        context.IsAcknowledged.ShouldBeFalse();

        await context.AcknowledgeAsync(ct);

        var ack = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>();
        ack.PacketType.ShouldBe(MqttPacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)11);
        ack.ReasonCode.ShouldBe(MqttReasonCode.Success);
        ack.ReasonString.ShouldBeNull();
    }

    [Fact]
    public async Task Acknowledged_inbound_qos2_publish_waits_for_consumer_ack_and_suppresses_pending_duplicates()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        var received = new TaskCompletionSource<MqttInboundPublishContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveries = 0;
        client.AcknowledgedMessageSink = (context, _) =>
        {
            Interlocked.Increment(ref deliveries);
            received.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        var publish = new MqttPublishPacket
        {
            Topic = "n",
            QualityOfService = MqttQualityOfService.ExactlyOnce,
            PacketIdentifier = 12,
        };
        await broker.SendAsync(publish, ct);

        var context = await received.Task.WaitAsync(SafetyTimeout);
        context.Message.Topic.ShouldBe("n");

        var pubRecRead = Task.Run(() => broker.ReadPacketAsync(ct), CancellationToken.None);
        (await Task.WhenAny(pubRecRead, Task.Delay(100, ct))).ShouldNotBe(pubRecRead);

        await broker.SendAsync(publish with { Dup = true }, ct);
        await Task.Delay(100, ct);
        Volatile.Read(ref deliveries).ShouldBe(1);
        pubRecRead.IsCompleted.ShouldBeFalse();

        await context.AcknowledgeAsync(ct);

        var pubRec = (await pubRecRead.WaitAsync(SafetyTimeout)).ShouldBeOfType<MqttPublishAckPacket>();
        pubRec.PacketType.ShouldBe(MqttPacketType.PubRec);
        pubRec.PacketIdentifier.ShouldBe((ushort)12);
    }

    [Fact]
    public async Task Inbound_qos2_duplicate_is_delivered_once()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var publish = new MqttPublishPacket
        {
            Topic = "n",
            QualityOfService = MqttQualityOfService.ExactlyOnce,
            PacketIdentifier = 5,
        };
        await broker.SendAsync(publish, ct);
        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>().PacketType.ShouldBe(MqttPacketType.PubRec);

        await broker.SendAsync(publish with { Dup = true }, ct);
        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>().PacketType.ShouldBe(MqttPacketType.PubRec);

        await broker.SendAsync(new MqttPublishAckPacket { PacketType = MqttPacketType.PubRel, PacketIdentifier = 5 }, ct);
        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishAckPacket>().PacketType.ShouldBe(MqttPacketType.PubComp);

        (await client.Messages.ReadAsync(ct)).Topic.ShouldBe("n");
        client.Messages.TryRead(out _).ShouldBeFalse(); // the duplicate was not delivered
    }

    [Fact]
    public async Task Subscribe_returns_the_granted_codes()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var subscribeTask = client.SubscribeAsync([new MqttTopicFilter("a/+")], ct);

        var seen = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket
            {
                PacketIdentifier = seen.PacketIdentifier,
                ReasonCodes = [MqttReasonCode.GrantedQualityOfService1],
            }, ct);

        (await subscribeTask).ShouldHaveSingleItem().ShouldBe(MqttReasonCode.GrantedQualityOfService1);
    }

    [Fact]
    public async Task Unsubscribe_returns_the_codes()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var unsubscribeTask = client.UnsubscribeAsync(["a/+"], ct);

        var seen = (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttUnsubscribePacket>();
        await broker.SendAsync(
            new MqttUnsubAckPacket { PacketIdentifier = seen.PacketIdentifier, ReasonCodes = [MqttReasonCode.Success] }, ct);

        (await unsubscribeTask).ShouldHaveSingleItem().ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task Pending_operations_fail_when_the_peer_closes()
    {
        var (client, broker, _, ct) = await ConnectedAsync();

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);
        await broker.ReadPacketAsync(ct);
        await broker.DisposeAsync();

        await Should.ThrowAsync<MqttException>(async () => await publishTask);
    }

    private static async Task<(RawMqttClient Client, ScriptedBroker Broker, FakeTimeProvider Time, CancellationToken Ct)> ConnectedAsync(
        MqttProtocolVersion protocolVersion = MqttProtocolVersion.V500)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        var client = new RawMqttClient(new FixedTransportFactory(clientTransport), timeProvider: time);

        var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(
            new MqttConnectPacket { ClientId = "c", KeepAliveSeconds = 0, ProtocolVersion = protocolVersion },
            timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { ProtocolVersion = protocolVersion }, timeout.Token);
        await connectTask;

        return (client, broker, time, timeout.Token);
    }

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
