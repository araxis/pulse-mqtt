using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class ReceiveMaximumTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    [Fact]
    public async Task Publishes_beyond_the_receive_maximum_wait_for_an_acknowledgement()
    {
        var (client, broker, timeout) = await ConnectedAsync(receiveMaximum: 2);

        var first = Publish(client, "t/1", timeout.Token);
        var second = Publish(client, "t/2", timeout.Token);
        var third = Publish(client, "t/3", timeout.Token);

        // Exactly two PUBLISH packets reach the broker; the third is gated client-side.
        var seen1 = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        var seen2 = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        third.IsCompleted.ShouldBeFalse();

        var pendingRead = broker.ReadPacketAsync(timeout.Token);
        await Task.Delay(100, timeout.Token);
        pendingRead.IsCompleted.ShouldBeFalse("the third PUBLISH must not hit the wire before an ack frees a slot");

        // Acknowledge one: the slot frees and the third PUBLISH flows.
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seen1.PacketIdentifier!.Value },
            timeout.Token);
        var seen3 = (await pendingRead).ShouldBeOfType<MqttPublishPacket>();

        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seen2.PacketIdentifier!.Value },
            timeout.Token);
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seen3.PacketIdentifier!.Value },
            timeout.Token);

        (await first).ShouldBe(MqttReasonCode.Success);
        (await second).ShouldBe(MqttReasonCode.Success);
        (await third).ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task A_cancelled_wait_releases_nothing_and_keeps_the_quota_intact()
    {
        var (client, broker, timeout) = await ConnectedAsync(receiveMaximum: 1);

        var first = Publish(client, "t/1", timeout.Token);
        var seen1 = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();

        using var cancelled = new CancellationTokenSource();
        var second = Publish(client, "t/2", cancelled.Token);
        cancelled.Cancel();
        await Should.ThrowAsync<OperationCanceledException>(async () => await second);

        // The slot still belongs to the first publish; acknowledging it frees the quota
        // and a new publish flows normally.
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seen1.PacketIdentifier!.Value },
            timeout.Token);
        (await first).ShouldBe(MqttReasonCode.Success);

        var third = Publish(client, "t/3", timeout.Token);
        var seen3 = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seen3.PacketIdentifier!.Value },
            timeout.Token);
        (await third).ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task A_qos2_slot_frees_at_pubcomp_not_at_pubrec()
    {
        var (client, broker, timeout) = await ConnectedAsync(receiveMaximum: 1);

        var qos2 = client.PublishAsync(
            new MqttPublishPacket { Topic = "t/q2", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.ExactlyOnce },
            timeout.Token);
        var seenQos2 = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();

        var qos1 = Publish(client, "t/q1", timeout.Token);
        var gatedRead = broker.ReadPacketAsync(timeout.Token);
        await Task.Delay(100, timeout.Token);
        gatedRead.IsCompleted.ShouldBeFalse("the QoS 1 publish must wait while the QoS 2 exchange holds the only slot");

        // PUBREC advances the exchange — the client sends PUBREL — but does NOT free the slot. Per
        // MQTT 5 section 4.9 the send quota frees only at PUBCOMP, the final acknowledgement, so the
        // QoS 1 publish keeps waiting.
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubRec, PacketIdentifier = seenQos2.PacketIdentifier!.Value },
            timeout.Token);

        var pubRel = (await gatedRead).ShouldBeOfType<MqttPublishAckPacket>();
        pubRel.PacketType.ShouldBe(MqttPacketType.PubRel);

        var stillGated = broker.ReadPacketAsync(timeout.Token);
        await Task.Delay(100, timeout.Token);
        stillGated.IsCompleted.ShouldBeFalse("the slot is still held by the QoS 2 exchange until its PUBCOMP");

        // PUBCOMP frees the slot; the QoS 1 publish now flows.
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubComp, PacketIdentifier = pubRel.PacketIdentifier },
            timeout.Token);

        var qos1Publish = (await stillGated).ShouldBeOfType<MqttPublishPacket>();
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = qos1Publish.PacketIdentifier!.Value },
            timeout.Token);

        (await qos2).ShouldBe(MqttReasonCode.Success);
        (await qos1).ShouldBe(MqttReasonCode.Success);
    }

    [Fact]
    public async Task No_advertised_receive_maximum_means_no_gating()
    {
        var (client, broker, timeout) = await ConnectedAsync(receiveMaximum: null);

        var tasks = Enumerable.Range(0, 5).Select(i => Publish(client, $"t/{i}", timeout.Token)).ToArray();

        // All five PUBLISH packets arrive without a single acknowledgement sent.
        var seen = new List<MqttPublishPacket>();
        for (var i = 0; i < 5; i++)
        {
            seen.Add((await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>());
        }

        foreach (var publish in seen)
        {
            await broker.SendAsync(
                new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = publish.PacketIdentifier!.Value },
                timeout.Token);
        }

        foreach (var task in tasks)
        {
            (await task).ShouldBe(MqttReasonCode.Success);
        }
    }

    [Fact]
    public async Task A_cancelled_publish_on_a_session_parks_its_quota_slot()
    {
        var session = new MqttInFlightSession((_, _) => ValueTask.CompletedTask);
        var (client, broker, timeout) = await ConnectedAsync(receiveMaximum: 1, session);

        using var cancelFirst = new CancellationTokenSource();
        var first = Publish(client, "a", cancelFirst.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>(); // on the wire, the only slot taken
        await cancelFirst.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(async () => await first);

        // The cancelled publish is parked in the session — the broker still counts it — so its slot stays
        // held. A second publish must wait rather than exceed the broker's Receive Maximum of 1.
        _ = Publish(client, "b", timeout.Token);
        var gated = broker.ReadPacketAsync(timeout.Token);
        await Task.Delay(100, timeout.Token);
        gated.IsCompleted.ShouldBeFalse("a cancelled-but-in-flight publish must keep its receive-maximum slot until the connection re-arms");
    }

    [Fact]
    public async Task A_publish_cancelled_during_its_completion_persist_still_settles_and_frees_the_slot()
    {
        var persistReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePersist = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sawSend = 0;
        var blocked = 0;

        // Block the completion persist (the removal after the ack, when Outbound is empty) once, after a
        // send has been recorded, honoring whatever token it is handed. A completion run on the caller's
        // token would throw here when the publish is cancelled; run on CancellationToken.None it will not.
        var session = new MqttInFlightSession(async (state, ct) =>
        {
            if (state.Outbound.Count > 0)
            {
                Interlocked.Exchange(ref sawSend, 1);
                return;
            }

            if (Volatile.Read(ref sawSend) == 1 && Interlocked.Exchange(ref blocked, 1) == 0)
            {
                persistReached.TrySetResult();
                await Task.WhenAny(releasePersist.Task, Task.Delay(Timeout.Infinite, ct)).ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
            }
        });

        var (client, broker, timeout) = await ConnectedAsync(receiveMaximum: 1, session);

        using var cancelPublish = new CancellationTokenSource();
        var publish = Publish(client, "a", cancelPublish.Token);
        var sent = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();

        // Acknowledge it: the exchange is now settled on the wire and the client runs its completion
        // bookkeeping, whose persist blocks below.
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = sent.PacketIdentifier!.Value },
            timeout.Token);
        await persistReached.Task.WaitAsync(timeout.Token);

        // Cancel the publish while its completion persist is in flight. The wire exchange is already
        // done, so completing it must not be abandoned: before the fix the completion ran on the caller
        // token, so this cancel threw and left settled=false, parking the id and the only receive-maximum
        // slot for the life of the connection (and PublishAsync threw instead of reporting the delivery).
        cancelPublish.Cancel();
        releasePersist.SetResult();

        (await publish).ShouldBe(MqttReasonCode.Success);

        // The slot the first publish held was released: a second publish flows instead of blocking on an
        // exhausted receive-maximum window of 1.
        var second = Publish(client, "b", timeout.Token);
        var sentB = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttPublishPacket>();
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = sentB.PacketIdentifier!.Value },
            timeout.Token);
        (await second).ShouldBe(MqttReasonCode.Success);
    }

    private static Task<MqttReasonCode> Publish(RawMqttClient client, string topic, CancellationToken cancellationToken) =>
        client.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            cancellationToken);

    private static async Task<(RawMqttClient Client, ScriptedBroker Broker, CancellationTokenSource Timeout)> ConnectedAsync(
        ushort? receiveMaximum,
        MqttInFlightSession? session = null)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var client = new RawMqttClient(new FixedTransportFactory(clientTransport), timeProvider: new FakeTimeProvider());

        var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(
            new MqttConnectPacket { ClientId = "c", CleanStart = session is null }, session, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { ReceiveMaximum = receiveMaximum }, timeout.Token);
        await connectTask;

        return (client, broker, timeout);
    }
}
