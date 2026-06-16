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

    // Each snapshot is stamped with a monotonic version under _gate (mutation order); persists are
    // serialized through _persistGate and applied only forward, so a slower persist of an older
    // snapshot can never overwrite a newer one in a durable async store.
    private readonly SemaphoreSlim _persistGate = new(1, 1);
    private long _version;
    private long _persistedVersion;

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
        long version;
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
            version = ++_version;
        }

        return PersistAsync(snapshot, version, cancellationToken);
    }

    /// <summary>Advances a QoS 2 exchange to <see cref="MqttInFlightStage.AwaitingPubComp"/> when its PUBREC arrives.</summary>
    public ValueTask OutboundAdvancedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        long version;
        lock (_gate)
        {
            var index = _outbound.FindIndex(entry => entry.Packet.PacketIdentifier == packetIdentifier);
            if (index >= 0)
            {
                _outbound[index] = _outbound[index] with { Stage = MqttInFlightStage.AwaitingPubComp };
            }

            snapshot = SnapshotLocked();
            version = ++_version;
        }

        return PersistAsync(snapshot, version, cancellationToken);
    }

    /// <summary>Removes a completed outbound exchange.</summary>
    public ValueTask OutboundCompletedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        long version;
        lock (_gate)
        {
            _outbound.RemoveAll(entry => entry.Packet.PacketIdentifier == packetIdentifier);
            snapshot = SnapshotLocked();
            version = ++_version;
        }

        return PersistAsync(snapshot, version, cancellationToken);
    }

    /// <summary>Records an inbound QoS 2 delivery for duplicate suppression across resumes.</summary>
    public ValueTask InboundReceivedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        long version;
        lock (_gate)
        {
            _inbound.Add(packetIdentifier);
            snapshot = SnapshotLocked();
            version = ++_version;
        }

        return PersistAsync(snapshot, version, cancellationToken);
    }

    /// <summary>Removes an inbound QoS 2 identifier once its PUBREL releases it.</summary>
    public ValueTask InboundReleasedAsync(ushort packetIdentifier, CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        long version;
        lock (_gate)
        {
            _inbound.Remove(packetIdentifier);
            snapshot = SnapshotLocked();
            version = ++_version;
        }

        return PersistAsync(snapshot, version, cancellationToken);
    }

    /// <summary>Discards everything — the broker did not preserve the session.</summary>
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        MqttInFlightState snapshot;
        long version;
        lock (_gate)
        {
            _outbound.Clear();
            _inbound.Clear();
            snapshot = SnapshotLocked();
            version = ++_version;
        }

        return PersistAsync(snapshot, version, cancellationToken);
    }

    // Builds the snapshot under the caller's lock so the persisted payload is bound to the mutation
    // that produced it. Concurrent mutations produce snapshots in version order under _gate; PersistAsync
    // then serializes the writes and applies them only forward.
    private MqttInFlightState SnapshotLocked() => new([.. _outbound], [.. _inbound]);

    // Serializes persists and drops a stale one. Concurrent mutations build snapshots in version order,
    // but their async persists can otherwise reach a durable store out of order and let an older snapshot
    // overwrite a newer one — resurrecting a completed exchange on the next restore (a spurious
    // redelivery). Applying only when the version advances keeps the store on the newest snapshot
    // regardless of completion order.
    private async ValueTask PersistAsync(MqttInFlightState snapshot, long version, CancellationToken cancellationToken)
    {
        await _persistGate.WaitScopedAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (version <= _persistedVersion)
            {
                return;
            }

            await _persist(snapshot, cancellationToken).ConfigureAwait(false);
            _persistedVersion = version;
        }
        finally
        {
            _persistGate.Release();
        }
    }
}
