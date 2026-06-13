using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Resilience;

/// <summary>The stage an unfinished outbound QoS exchange is waiting at.</summary>
public enum MqttInFlightStage
{
    /// <summary>The QoS 1 PUBLISH went out; no PUBACK yet. Resume re-sends the PUBLISH with DUP.</summary>
    AwaitingPubAck,

    /// <summary>The QoS 2 PUBLISH went out; no PUBREC yet. Resume re-sends the PUBLISH with DUP.</summary>
    AwaitingPubRec,

    /// <summary>The QoS 2 PUBREC arrived; PUBCOMP is outstanding. Resume re-sends the PUBREL only.</summary>
    AwaitingPubComp,
}

/// <summary>One unfinished outbound exchange: the identified PUBLISH and where it stopped.</summary>
public sealed record MqttInFlightPublish(MqttPublishPacket Packet, MqttInFlightStage Stage);

/// <summary>A snapshot of the session's unfinished QoS state, as a store persists it.</summary>
/// <param name="Outbound">Unfinished outbound exchanges, oldest first — the redelivery order.</param>
/// <param name="InboundExactlyOnce">Inbound QoS 2 packet identifiers already delivered but not yet released.</param>
public sealed record MqttInFlightState(
    IReadOnlyList<MqttInFlightPublish> Outbound,
    IReadOnlyList<ushort> InboundExactlyOnce);

/// <summary>
/// Tracks the QoS exchanges a persistent session must resume: outbound publishes that have not
/// completed their acknowledgement flow, and inbound QoS 2 identifiers needed for duplicate
/// suppression. The resilient client owns one per session and hands it to each connection;
/// every mutation flows through the persist callback so a durable store always holds the
/// current state.
/// </summary>
public sealed class MqttInFlightSession
{
    private readonly List<MqttInFlightPublish> _outbound = [];
    private readonly HashSet<ushort> _inbound = [];
    private readonly Func<MqttInFlightState, CancellationToken, ValueTask> _persist;
    private readonly object _gate = new();

    /// <summary>Creates a session that persists every change through <paramref name="persist"/>.</summary>
    public MqttInFlightSession(Func<MqttInFlightState, CancellationToken, ValueTask> persist)
    {
        ArgumentNullException.ThrowIfNull(persist);
        _persist = persist;
    }

    /// <summary>Replaces the live state from a stored snapshot (start-up restore).</summary>
    public void Restore(MqttInFlightState? state)
    {
        lock (_gate)
        {
            _outbound.Clear();
            _inbound.Clear();
            if (state is null)
            {
                return;
            }

            _outbound.AddRange(state.Outbound);
            foreach (var id in state.InboundExactlyOnce)
            {
                _inbound.Add(id);
            }
        }
    }

    /// <summary>The current state, oldest outbound exchange first.</summary>
    public MqttInFlightState Snapshot()
    {
        lock (_gate)
        {
            return SnapshotLocked();
        }
    }

    /// <summary>Records an outbound QoS 1/2 PUBLISH the moment it is handed to the wire.</summary>
    public ValueTask OutboundSentAsync(MqttPublishPacket packet, MqttInFlightStage stage, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        lock (_gate)
        {
            // Update in place when the identifier is already tracked (a redelivery re-records
            // it) so the entry keeps its original position — redelivery order is the list order.
            var index = _outbound.FindIndex(entry => entry.Packet.PacketIdentifier == packet.PacketIdentifier);
            var updated = new MqttInFlightPublish(packet, stage);
            if (index >= 0)
            {
                _outbound[index] = updated;
            }
            else
            {
                _outbound.Add(updated);
            }

            snapshot = SnapshotLocked();
        }

        return _persist(snapshot, cancellationToken);
    }

    /// <summary>Advances a QoS 2 exchange to <see cref="MqttInFlightStage.AwaitingPubComp"/> when its PUBREC arrives.</summary>
    public ValueTask OutboundAdvancedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        lock (_gate)
        {
            var index = _outbound.FindIndex(entry => entry.Packet.PacketIdentifier == packetIdentifier);
            if (index >= 0)
            {
                _outbound[index] = _outbound[index] with { Stage = MqttInFlightStage.AwaitingPubComp };
            }

            snapshot = SnapshotLocked();
        }

        return _persist(snapshot, cancellationToken);
    }

    /// <summary>Removes a completed outbound exchange.</summary>
    public ValueTask OutboundCompletedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        lock (_gate)
        {
            _outbound.RemoveAll(entry => entry.Packet.PacketIdentifier == packetIdentifier);
            snapshot = SnapshotLocked();
        }

        return _persist(snapshot, cancellationToken);
    }

    /// <summary>Records an inbound QoS 2 delivery for duplicate suppression across resumes.</summary>
    public ValueTask InboundReceivedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        lock (_gate)
        {
            _inbound.Add(packetIdentifier);
            snapshot = SnapshotLocked();
        }

        return _persist(snapshot, cancellationToken);
    }

    /// <summary>Removes an inbound QoS 2 identifier once its PUBREL releases it.</summary>
    public ValueTask InboundReleasedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        lock (_gate)
        {
            _inbound.Remove(packetIdentifier);
            snapshot = SnapshotLocked();
        }

        return _persist(snapshot, cancellationToken);
    }

    /// <summary>Discards everything — the broker did not preserve the session.</summary>
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        lock (_gate)
        {
            _outbound.Clear();
            _inbound.Clear();
            snapshot = SnapshotLocked();
        }

        return _persist(snapshot, cancellationToken);
    }

    // Builds the snapshot under the caller's lock so the persisted payload is bound to the
    // mutation that produced it. NOTE for durable async stores: persists are not yet serialized
    // across concurrent mutations — add a version stamp or single-writer queue before shipping a
    // store whose SaveInFlightAsync completes asynchronously.
    private MqttInFlightState SnapshotLocked() => new([.. _outbound], [.. _inbound]);
}
