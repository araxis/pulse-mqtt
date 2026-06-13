using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class MqttInFlightSessionTests
{
    private static MqttPublishPacket Publish(ushort id) =>
        new() { Topic = "t", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = id };

    [Fact]
    public async Task Every_mutation_persists_the_current_snapshot()
    {
        MqttInFlightState? persisted = null;
        var session = new MqttInFlightSession((state, _) => { persisted = state; return ValueTask.CompletedTask; });

        await session.OutboundSentAsync(Publish(1), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);
        persisted!.Outbound.ShouldHaveSingleItem().Packet.PacketIdentifier.ShouldBe((ushort)1);

        await session.OutboundCompletedAsync(1, CancellationToken.None);
        persisted!.Outbound.ShouldBeEmpty();
    }

    [Fact]
    public async Task Outbound_entries_keep_insertion_order_and_advance_in_place()
    {
        var session = new MqttInFlightSession((_, _) => ValueTask.CompletedTask);

        await session.OutboundSentAsync(Publish(5), MqttInFlightStage.AwaitingPubRec, CancellationToken.None);
        await session.OutboundSentAsync(Publish(6), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);
        await session.OutboundAdvancedAsync(5, CancellationToken.None);

        var snapshot = session.Snapshot();
        snapshot.Outbound.Select(e => e.Packet.PacketIdentifier).ShouldBe([(ushort)5, (ushort)6]);
        snapshot.Outbound[0].Stage.ShouldBe(MqttInFlightStage.AwaitingPubComp);
        snapshot.Outbound[1].Stage.ShouldBe(MqttInFlightStage.AwaitingPubAck);
    }

    [Fact]
    public async Task Re_sending_the_same_identifier_replaces_rather_than_duplicates()
    {
        var session = new MqttInFlightSession((_, _) => ValueTask.CompletedTask);

        await session.OutboundSentAsync(Publish(9), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);
        await session.OutboundSentAsync(Publish(9), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);

        session.Snapshot().Outbound.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task Re_recording_an_existing_identifier_keeps_its_position()
    {
        var session = new MqttInFlightSession((_, _) => ValueTask.CompletedTask);

        await session.OutboundSentAsync(Publish(1), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);
        await session.OutboundSentAsync(Publish(2), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);
        // A redelivery re-records id 1; it must stay first, not jump to the end of the order.
        await session.OutboundSentAsync(Publish(1), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);

        session.Snapshot().Outbound.Select(e => e.Packet.PacketIdentifier).ShouldBe([(ushort)1, (ushort)2]);
    }

    [Fact]
    public async Task The_persisted_snapshot_reflects_the_mutation_that_triggered_it()
    {
        var persisted = new List<MqttInFlightState>();
        var session = new MqttInFlightSession((state, _) => { lock (persisted) { persisted.Add(state); } return ValueTask.CompletedTask; });

        await session.OutboundSentAsync(Publish(1), MqttInFlightStage.AwaitingPubAck, CancellationToken.None);
        await session.InboundReceivedAsync(7, CancellationToken.None);
        await session.OutboundCompletedAsync(1, CancellationToken.None);

        // Each persisted payload is the exact post-mutation state, captured under the lock.
        persisted[0].Outbound.ShouldHaveSingleItem();
        persisted[1].InboundExactlyOnce.ShouldBe([(ushort)7]);
        persisted[2].Outbound.ShouldBeEmpty();
        persisted[2].InboundExactlyOnce.ShouldBe([(ushort)7]);
    }

    [Fact]
    public async Task Inbound_identifiers_track_for_duplicate_suppression()
    {
        var session = new MqttInFlightSession((_, _) => ValueTask.CompletedTask);

        await session.InboundReceivedAsync(3, CancellationToken.None);
        await session.InboundReceivedAsync(4, CancellationToken.None);
        await session.InboundReleasedAsync(3, CancellationToken.None);

        session.Snapshot().InboundExactlyOnce.ShouldBe([(ushort)4]);
    }

    [Fact]
    public async Task Restore_rehydrates_then_clear_empties()
    {
        var session = new MqttInFlightSession((_, _) => ValueTask.CompletedTask);

        session.Restore(new MqttInFlightState(
            [new MqttInFlightPublish(Publish(7), MqttInFlightStage.AwaitingPubComp)],
            [(ushort)2]));

        var restored = session.Snapshot();
        restored.Outbound.ShouldHaveSingleItem().Stage.ShouldBe(MqttInFlightStage.AwaitingPubComp);
        restored.InboundExactlyOnce.ShouldBe([(ushort)2]);

        await session.ClearAsync(CancellationToken.None);
        var cleared = session.Snapshot();
        cleared.Outbound.ShouldBeEmpty();
        cleared.InboundExactlyOnce.ShouldBeEmpty();
    }
}
