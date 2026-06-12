using BenchmarkDotNet.Attributes;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// The send-path synchronization primitive (<see cref="SemaphoreSlim"/>, used to serialize
/// concurrent senders on a connection): contended hand-off across 100 tasks and uncontended
/// acquire/release throughput.
/// </summary>
[MemoryDiagnoser]
public class SendLockBenchmarks
{
    [Benchmark]
    public async Task Synchronize_100_Tasks()
    {
        const int taskCount = 100;

        var tasks = new Task[taskCount];
        using var gate = new SemaphoreSlim(1, 1);
        var shared = 0;

        for (var i = 0; i < taskCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                await gate.WaitAsync().ConfigureAwait(false);
                try
                {
                    var local = shared;
                    await Task.Delay(5).ConfigureAwait(false);
                    shared = local + 1;
                }
                finally
                {
                    gate.Release();
                }
            });
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        if (shared != taskCount)
        {
            throw new InvalidOperationException($"Lost update: {shared} of {taskCount}.");
        }
    }

    [Benchmark]
    public async Task Wait_100_000_Times()
    {
        using var gate = new SemaphoreSlim(1, 1);
        using var cancellation = new CancellationTokenSource();

        for (var i = 0; i < 100_000; i++)
        {
            await gate.WaitAsync(cancellation.Token).ConfigureAwait(false);
            gate.Release();
        }
    }
}
