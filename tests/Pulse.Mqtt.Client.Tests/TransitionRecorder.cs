using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>
/// Collects state transitions from the event thread and lets the test await a condition over
/// everything recorded so far, without racing the recording or enumerating a live list.
/// </summary>
internal sealed class TransitionRecorder
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
