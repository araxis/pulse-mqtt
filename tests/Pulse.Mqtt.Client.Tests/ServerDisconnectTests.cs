using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class ServerDisconnectTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    // These tests synchronize on the StateChanged *event*, not on polling client.State: the state
    // property is published before the event fires, so a state poll can win the race and assert
    // on the transition list before the matching entry was recorded.

    [Fact]
    public async Task A_transient_broker_disconnect_reconnects_and_carries_the_reason()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Connected), timeout.Token);

        await broker1.SendAsync(new MqttDisconnectPacket { ReasonCode = MqttReasonCode.ServerShuttingDown }, timeout.Token);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Count(change => change.Current == ConnectionState.Connected) == 2, timeout.Token);

        transitions.Snapshot().ShouldContain(change =>
            change.Current == ConnectionState.Reconnecting &&
            change.Reason == MqttReasonCode.ServerShuttingDown &&
            change.Error is MqttServerDisconnectedException);
    }

    [Fact]
    public async Task A_terminal_broker_disconnect_faults_instead_of_reconnecting()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Connected), timeout.Token);

        await broker.SendAsync(new MqttDisconnectPacket { ReasonCode = MqttReasonCode.SessionTakenOver }, timeout.Token);

        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Faulted), timeout.Token);
        factory.ConnectionsHandedOut.ShouldBe(1); // no reconnect attempt — the session has a new owner
        transitions.Snapshot().ShouldContain(change =>
            change.Current == ConnectionState.Faulted &&
            change.Reason == MqttReasonCode.SessionTakenOver &&
            change.Error is MqttServerDisconnectedException);
    }

    [Fact]
    public async Task The_lifecycle_sees_the_disconnect_details_before_the_fault()
    {
        var factory = new SequencedTransportFactory();
        var lifecycle = new RecordingLifecycle();
        var transitions = new TransitionRecorder();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "down-context", KeepAliveSeconds = 0 },
            Lifecycle = lifecycle,
        });
        client.StateChanged += transitions.Record;

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Connected), timeout.Token);

        await broker.SendAsync(
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ReasonString = "rebalancing",
                ServerReference = "backup.example:1883",
            },
            timeout.Token);

        // The lifecycle callback completes before the Faulted transition is raised, so waiting
        // for the Faulted event also guarantees LastDown is visible.
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Faulted), timeout.Token);

        var context = lifecycle.LastDown.ShouldNotBeNull();
        context.Reason.ShouldBe(MqttReasonCode.UseAnotherServer);
        context.ReasonString.ShouldBe("rebalancing");
        context.ServerReference.ShouldBe("backup.example:1883");
        context.Error.ShouldBeOfType<MqttServerDisconnectedException>();

        transitions.Snapshot().ShouldContain(change =>
            change.Current == ConnectionState.Faulted &&
            change.Reason == MqttReasonCode.UseAnotherServer &&
            change.ReasonString == "rebalancing" &&
            change.ServerReference == "backup.example:1883" &&
            change.Error is MqttServerDisconnectedException);
    }

    private sealed class RecordingLifecycle : IConnectionLifecycle
    {
        public IConnectionDownContext? LastDown { get; private set; }

        public ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask OnConnectionDownAsync(IConnectionDownContext context, CancellationToken cancellationToken)
        {
            LastDown = context;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Collects state transitions from the event thread and lets the test await a condition over
    /// everything recorded so far, without racing the recording or enumerating a live list.
    /// </summary>
    private sealed class TransitionRecorder
    {
        private readonly Lock _gate = new();
        private readonly List<ConnectionStateChanged> _changes = [];
        private readonly List<(Func<IReadOnlyList<ConnectionStateChanged>, bool> Condition, TaskCompletionSource Signal)> _waiters = [];

        public void Record(ConnectionStateChanged change)
        {
            lock (_gate)
            {
                _changes.Add(change);
                for (var i = _waiters.Count - 1; i >= 0; i--)
                {
                    if (_waiters[i].Condition(_changes))
                    {
                        _waiters[i].Signal.TrySetResult();
                        _waiters.RemoveAt(i);
                    }
                }
            }
        }

        public ConnectionStateChanged[] Snapshot()
        {
            lock (_gate)
            {
                return [.. _changes];
            }
        }

        public Task WaitForAsync(Func<IReadOnlyList<ConnectionStateChanged>, bool> condition, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                if (condition(_changes))
                {
                    return Task.CompletedTask;
                }

                var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add((condition, signal));
                return signal.Task.WaitAsync(cancellationToken);
            }
        }
    }

    private static ResilientMqttClient NewClient(SequencedTransportFactory factory, out TransitionRecorder transitions)
    {
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "disconnect-tests", KeepAliveSeconds = 0 },
        });

        var recorder = new TransitionRecorder();
        client.StateChanged += recorder.Record;
        transitions = recorder;
        return client;
    }
}
