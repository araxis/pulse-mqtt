using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class SessionRedeliveryTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_qos1_publish_dropped_before_puback_redelivers_with_dup_on_resume()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);

        var sent = (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        sent.Dup.ShouldBeFalse();

        // Drop mid-exchange: the caller learns it is in flight, not lost.
        await broker1.DisposeAsync();
        (await publishTask).Disposition.ShouldBe(PublishDisposition.InFlight);

        // Resume with the same session: the PUBLISH redelivers with DUP and the same id.
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);

        var redelivered = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        redelivered.Dup.ShouldBeTrue();
        redelivered.PacketIdentifier.ShouldBe(sent.PacketIdentifier);

        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = redelivered.PacketIdentifier!.Value },
            timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task A_qos2_publish_dropped_before_pubrec_redelivers_the_publish_with_dup()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.ExactlyOnce },
            timeout.Token);
        var sent = (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();

        await broker1.DisposeAsync();
        (await publishTask).Disposition.ShouldBe(PublishDisposition.InFlight);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);

        var redelivered = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        redelivered.Dup.ShouldBeTrue();
        redelivered.QualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);

        // Complete the four-packet exchange on the new connection.
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubRec, PacketIdentifier = sent.PacketIdentifier!.Value },
            timeout.Token);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubRel);
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubComp, PacketIdentifier = sent.PacketIdentifier!.Value },
            timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task A_flushed_publish_left_in_flight_is_removed_from_the_offline_queue()
    {
        var store = new InMemoryMessageStore(new OfflineQueueOptions { Capacity = 8 });
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "flush-inflight", KeepAliveSeconds = 0, CleanStart = false, SessionExpiryInterval = 300 },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(5) },
            MessageStore = store,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // Drop the live connection; the client starts reconnecting.
        await broker1.DisposeAsync();
        while (factory.ConnectionsHandedOut < 2)
        {
            await Task.Delay(5, timeout.Token);
        }

        // A QoS 2 publish while offline lands in the durable queue.
        _ = client.PublishAsync(
            new MqttPublishPacket { Topic = "q", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.ExactlyOnce },
            timeout.Token);
        await WaitForAsync(() => store.Count == 1, timeout.Token);

        // Resume the session: the connection-up flush publishes the queued message, which the client
        // now awaits (QoS 2 exchange in flight).
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>().Topic.ShouldBe("q");

        // The connection dies before the PUBREC: the publish is now tracked by the session, which will
        // redeliver it with DUP on the next resume. It must therefore be removed from the offline
        // queue — otherwise the next connection-up flush would re-publish it under a fresh identifier
        // while the session redelivers the original, double-sending it (a QoS 2 exactly-once
        // violation). Before the fix the MqttPublishInFlightException escaped the flush and skipped the
        // removal, so the entry stayed queued and store.Count never returned to zero.
        await broker2.DisposeAsync();
        await WaitForAsync(() => store.Count == 0, timeout.Token);
    }

    [Fact]
    public async Task A_qos2_publish_dropped_after_pubrec_redelivers_only_the_pubrel()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.ExactlyOnce },
            timeout.Token);
        var sent = (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();

        // Drive the exchange to PUBREL, then drop before PUBCOMP — stage is AwaitingPubComp.
        await broker1.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubRec, PacketIdentifier = sent.PacketIdentifier!.Value },
            timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubRel);

        await broker1.DisposeAsync();
        (await publishTask).Disposition.ShouldBe(PublishDisposition.InFlight);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);

        // Redelivery resends the PUBREL only — never the PUBLISH again.
        var redelivered = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>();
        redelivered.PacketType.ShouldBe(MqttPacketType.PubRel);
        redelivered.PacketIdentifier.ShouldBe(sent.PacketIdentifier!.Value);

        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubComp, PacketIdentifier = sent.PacketIdentifier!.Value },
            timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task A_clean_session_on_resume_discards_in_flight_state()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "lost", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await broker1.DisposeAsync();
        (await publishTask).Disposition.ShouldBe(PublishDisposition.InFlight);

        // The broker reports a fresh session: in-flight state is discarded per spec.
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: false);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // No redelivery happened: the next thing the broker sees is a brand-new publish.
        var fresh = client.PublishAsync(
            new MqttPublishPacket { Topic = "fresh", Payload = new byte[] { 2 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        var seen = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        seen.Topic.ShouldBe("fresh");
        seen.Dup.ShouldBeFalse();
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = seen.PacketIdentifier!.Value },
            timeout.Token);
        await fresh;
    }

    [Fact]
    public async Task Inbound_qos2_duplicate_suppression_survives_the_reconnect()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // The broker sends a QoS 2 message; the client delivers it once and answers PUBREC,
        // then the connection drops before the PUBREL.
        await broker1.SendAsync(
            new MqttPublishPacket { Topic = "in", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.ExactlyOnce, PacketIdentifier = 50 },
            timeout.Token);
        (await client.Messages.ReadAsync(timeout.Token)).Topic.ShouldBe("in");
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubRec);

        await broker1.DisposeAsync();

        // Resume the session; the broker redelivers the same QoS 2 message with DUP.
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        await broker2.SendAsync(
            new MqttPublishPacket { Topic = "in", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.ExactlyOnce, PacketIdentifier = 50, Dup = true },
            timeout.Token);

        // The client answers PUBREC again but must NOT deliver the message a second time.
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubRec);
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubRel, PacketIdentifier = 50 },
            timeout.Token);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubComp);

        client.Messages.TryRead(out _).ShouldBeFalse("the duplicate must not be delivered twice");
    }

    [Fact]
    public async Task Acknowledged_inbound_qos2_redelivers_after_reconnect_before_consumer_ack()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await using var stream = client.OpenAcknowledgedRouteStream("in");
        await broker1.SendAsync(
            new MqttPublishPacket
            {
                Topic = "in",
                Payload = new byte[] { 1 },
                QualityOfService = MqttQualityOfService.ExactlyOnce,
                PacketIdentifier = 51,
            },
            timeout.Token);

        var first = await stream.Reader.ReadAsync(timeout.Token);
        first.Message.Topic.ShouldBe("in");
        first.IsAcknowledged.ShouldBeFalse();

        // The application has the message, but has not acknowledged it. A reconnect before PUBREC
        // must not turn the packet id into durable duplicate-suppression state.
        await broker1.DisposeAsync();

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        await broker2.SendAsync(
            new MqttPublishPacket
            {
                Topic = "in",
                Payload = new byte[] { 1 },
                QualityOfService = MqttQualityOfService.ExactlyOnce,
                PacketIdentifier = 51,
                Dup = true,
            },
            timeout.Token);

        var second = await stream.Reader.ReadAsync(timeout.Token);
        second.Message.Topic.ShouldBe("in");

        await second.AcknowledgeAsync(timeout.Token);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubRec);

        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubRel, PacketIdentifier = 51 },
            timeout.Token);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>()
            .PacketType.ShouldBe(MqttPacketType.PubComp);
    }

    [Fact]
    public async Task A_swapped_session_store_persists_the_in_flight_state()
    {
        var store = new InMemorySessionStore();
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "persisted", KeepAliveSeconds = 0, CleanStart = false },
            SessionStore = store,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        _ = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();

        // The store now holds the unfinished exchange.
        var state = await store.LoadInFlightAsync(timeout.Token);
        state.ShouldNotBeNull().Outbound.ShouldHaveSingleItem().Packet.Topic.ShouldBe("t");
    }

    [Fact]
    public async Task A_cancelled_publish_keeps_its_entry_and_its_id_is_not_recycled()
    {
        var store = new InMemorySessionStore();
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "cancel", KeepAliveSeconds = 0, CleanStart = false },
            SessionStore = store,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // Cancel a QoS 1 publish after its PUBLISH is on the wire but before the PUBACK.
        using var cancelFirst = new CancellationTokenSource();
        var first = client.PublishAsync(
            new MqttPublishPacket { Topic = "a", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            cancelFirst.Token);
        var sentA = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        cancelFirst.Cancel();
        await Should.ThrowAsync<OperationCanceledException>(async () => await first);

        // The next publish must NOT reuse the cancelled id (it is still reserved for redelivery),
        // and the cancelled message must still be tracked in the session.
        var second = client.PublishAsync(
            new MqttPublishPacket { Topic = "b", Payload = new byte[] { 2 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        var sentB = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        sentB.PacketIdentifier.ShouldNotBe(sentA.PacketIdentifier);

        var state = await store.LoadInFlightAsync(timeout.Token);
        state.ShouldNotBeNull().Outbound.ShouldContain(e => e.Packet.Topic == "a" && e.Packet.PacketIdentifier == sentA.PacketIdentifier);

        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = sentB.PacketIdentifier!.Value },
            timeout.Token);
        (await second).Disposition.ShouldBe(PublishDisposition.Delivered);
    }

    [Fact]
    public async Task Redelivery_drops_a_too_large_entry_and_keeps_delivering_the_rest()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // Two QoS 1 publishes in flight: one large, one small.
        var big = client.PublishAsync(
            new MqttPublishPacket { Topic = "big", Payload = new byte[64], QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        var small = client.PublishAsync(
            new MqttPublishPacket { Topic = "small", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();

        await broker1.DisposeAsync();
        (await big).Disposition.ShouldBe(PublishDisposition.InFlight);
        (await small).Disposition.ShouldBe(PublishDisposition.InFlight);

        // The resumed broker accepts only small packets: redelivery drops the big one and still
        // redelivers the small one, then reaches Connected (no abort, no re-loop).
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true, maximumPacketSize: 32);

        var redelivered = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        redelivered.Topic.ShouldBe("small");
        redelivered.Dup.ShouldBeTrue();
        await broker2.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = redelivered.PacketIdentifier!.Value },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    private static ResilientMqttClient NewClient(SequencedTransportFactory factory) =>
        new(factory, new ResilientMqttClientOptions
        {
            // CleanStart = false signals the intent to resume — the trigger for in-flight tracking.
            Connect = new MqttConnectPacket { ClientId = "redeliver", KeepAliveSeconds = 0, CleanStart = false },
        });

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(1, cancellationToken);
        }
    }

    private static async Task WaitForAsync(Func<bool> predicate, CancellationToken cancellationToken)
    {
        while (!predicate())
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
