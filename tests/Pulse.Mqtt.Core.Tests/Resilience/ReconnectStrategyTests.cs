using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class ReconnectStrategyTests
{
    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }

    private sealed class RecordingContext(TimeProvider time, List<int> starts) : IReconnectContext
    {
        public int Attempt { get; private set; }

        public TimeProvider Time => time;

        public void OnAttemptStarting(int attempt)
        {
            Attempt = attempt;
            starts.Add(attempt);
        }

        public void OnAttemptFailed(int attempt, Exception error)
        {
        }
    }

    private static BackoffReconnectStrategy NewBackoff(BackoffOptions? options = null) =>
        new(options ?? new BackoffOptions(), new DefaultReconnectDecision(), new FixedRandom(0.5));

    [Fact]
    public async Task Backoff_succeeds_after_transient_failures_and_counts_attempts()
    {
        var time = new FakeTimeProvider();
        var starts = new List<int>();
        var failures = 2;

        Task ConnectOnce(CancellationToken ct)
        {
            if (failures-- > 0)
            {
                throw new MqttException("transient");
            }

            return Task.CompletedTask;
        }

        var run = NewBackoff().RunAsync(ConnectOnce, new RecordingContext(time, starts), CancellationToken.None);
        await AdvanceUntilAsync(time, () => run.IsCompleted);
        await run;

        starts.ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task Backoff_rethrows_a_pre_classified_terminal_failure_without_retry()
    {
        var starts = new List<int>();

        static Task ConnectOnce(CancellationToken ct) => throw new TerminalMqttConnectException("nope");

        await Should.ThrowAsync<TerminalMqttConnectException>(
            async () => await NewBackoff().RunAsync(ConnectOnce, new RecordingContext(new FakeTimeProvider(), starts), CancellationToken.None));

        starts.ShouldBe([1]);
    }

    [Fact]
    public async Task Backoff_turns_a_terminal_reason_into_a_terminal_exception()
    {
        static Task ConnectOnce(CancellationToken ct) => throw new MqttConnectRejectedException(MqttReasonCode.NotAuthorized);

        var thrown = await Should.ThrowAsync<TerminalMqttConnectException>(
            async () => await NewBackoff().RunAsync(ConnectOnce, new RecordingContext(new FakeTimeProvider(), []), CancellationToken.None));

        thrown.InnerException.ShouldBeOfType<MqttConnectRejectedException>().ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
    }

    [Fact]
    public async Task Backoff_gives_up_after_the_attempt_cap()
    {
        var time = new FakeTimeProvider();
        var starts = new List<int>();

        static Task ConnectOnce(CancellationToken ct) => throw new MqttException("transient");

        var run = NewBackoff(new BackoffOptions { MaxAttempts = 3 })
            .RunAsync(ConnectOnce, new RecordingContext(time, starts), CancellationToken.None);
        await AdvanceUntilAsync(time, () => run.IsCompleted);

        await Should.ThrowAsync<TerminalMqttConnectException>(async () => await run);
        starts.ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task No_reconnect_attempts_once_and_rethrows()
    {
        var starts = new List<int>();

        static Task ConnectOnce(CancellationToken ct) => throw new MqttException("boom");

        await Should.ThrowAsync<MqttException>(
            async () => await new NoReconnectStrategy().RunAsync(ConnectOnce, new RecordingContext(new FakeTimeProvider(), starts), CancellationToken.None));

        starts.ShouldBe([1]);
    }

    [Fact]
    public async Task No_reconnect_returns_on_success()
    {
        var starts = new List<int>();

        await new NoReconnectStrategy().RunAsync(
            _ => Task.CompletedTask, new RecordingContext(new FakeTimeProvider(), starts), CancellationToken.None);

        starts.ShouldBe([1]);
    }

    private static async Task AdvanceUntilAsync(FakeTimeProvider time, Func<bool> done)
    {
        for (var i = 0; i < 200 && !done(); i++)
        {
            time.Advance(TimeSpan.FromSeconds(31));
            await Task.Delay(5);
        }

        done().ShouldBeTrue();
    }
}
