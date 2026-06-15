using System.IO.Pipelines;
using System.Reflection;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

/// <summary>
/// Tearing a connection down while a send is queued behind another send must not leave a dangling
/// cancellation registration on the long-lived caller token. The regression: the send lock was
/// awaited with the caller's token, so a send still queued on the lock when the connection (and its
/// lock) was disposed orphaned a registration that pointed into the disposed lock. Cancelling that
/// long-lived token later — as a soak's lifetime token does at the end — then threw an
/// <see cref="AggregateException"/> of <see cref="ObjectDisposedException"/> and crashed the process.
/// </summary>
public sealed class SendLockTeardownTests
{
    [Fact]
    public async Task Disposing_with_a_send_queued_on_the_lock_leaves_no_registration_on_the_caller_token()
    {
        var transport = new BlockingOutputTransport();
        var connection = new MqttConnection(transport, new MqttConnectionOptions());
        connection.Start();

        using var caller = new CancellationTokenSource();

        // Send #1 acquires the send lock and parks inside FlushAsync (the transport never completes
        // the flush), holding the lock for the rest of the test.
        var first = connection.SendAsync(new MqttPingReqPacket(), CancellationToken.None).AsTask();
        Observe(first);
        (await Task.WhenAny(transport.FlushEntered.Task, Task.Delay(TimeSpan.FromSeconds(5))))
            .ShouldBe(transport.FlushEntered.Task, "the first send never reached the transport flush");

        // Send #2 queues on the held lock with the caller's token.
        var second = connection.SendAsync(new MqttPingReqPacket(), caller.Token).AsTask();
        Observe(second);
        await Task.Delay(100); // let it reach the lock wait

        // Chaos teardown: the connection (and its send lock) is disposed while send #2 is queued.
        await connection.DisposeAsync();

        // The send lock is acquired through WaitScopedAsync, which keeps SemaphoreSlim's internal
        // linked source off the caller token. So tearing down mid-wait leaves at most the one
        // per-call scoped registration (not a growing pile pointing into the disposed lock), and
        // cancelling the caller token executes cleanly instead of throwing ObjectDisposedException.
        Should.NotThrow(caller.Cancel);
        CountRegistrations(caller).ShouldBeLessThan(3);
    }

    // Swallows the task's eventual outcome so a send abandoned by teardown is not an unobserved exception.
    private static void Observe(Task task) => _ = task.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);

    private static int CountRegistrations(CancellationTokenSource cts)
    {
        var registrations = typeof(CancellationTokenSource)
            .GetField("_registrations", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(cts);
        if (registrations is null)
        {
            return 0;
        }

        var node = registrations.GetType().GetField("Callbacks", BindingFlags.Public | BindingFlags.Instance)!.GetValue(registrations);
        var nextField = node?.GetType().GetField("Next", BindingFlags.Public | BindingFlags.Instance);

        var count = 0;
        while (node is not null && nextField is not null)
        {
            count++;
            node = nextField.GetValue(node);
        }

        return count;
    }

    // A transport whose Output parks in FlushAsync until released (which the test never does), so the
    // first send holds the connection's send lock for the whole test. Input stays idle.
    private sealed class BlockingOutputTransport : IMqttTransport
    {
        private readonly Pipe _inbound = new();
        private readonly BlockingWriter _output = new();

        public TaskCompletionSource FlushEntered => _output.FlushEntered;

        public PipeReader Input => _inbound.Reader;

        public PipeWriter Output => _output;

        public ValueTask DisposeAsync()
        {
            _inbound.Writer.Complete();
            return ValueTask.CompletedTask;
        }

        private sealed class BlockingWriter : PipeWriter
        {
            private readonly byte[] _scratch = new byte[64 * 1024];
            private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _written;

            public TaskCompletionSource FlushEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public override void Advance(int bytes) => _written += bytes;

            public override Memory<byte> GetMemory(int sizeHint = 0) => _scratch.AsMemory(_written);

            public override Span<byte> GetSpan(int sizeHint = 0) => _scratch.AsSpan(_written);

            public override void CancelPendingFlush() { }

            public override void Complete(Exception? exception = null) { }

            public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                FlushEntered.TrySetResult();
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return new FlushResult(isCanceled: false, isCompleted: false);
            }
        }
    }
}
