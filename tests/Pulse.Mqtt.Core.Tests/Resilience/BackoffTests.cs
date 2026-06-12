using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Resilience;

public sealed class BackoffTests
{
    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }

    private static readonly BackoffOptions Options = new()
    {
        BaseDelay = TimeSpan.FromMilliseconds(500),
        MaxDelay = TimeSpan.FromSeconds(30),
    };

    [Theory]
    [InlineData(1, 500)]
    [InlineData(2, 1000)]
    [InlineData(3, 2000)]
    [InlineData(6, 16000)]
    public void Full_jitter_reaches_the_exponential_ceiling(int attempt, double expectedCeilingMs)
    {
        var delay = Backoff.Compute(attempt, Options, new FixedRandom(1.0));

        delay.TotalMilliseconds.ShouldBe(expectedCeilingMs, tolerance: 0.001);
    }

    [Fact]
    public void Jitter_can_be_zero()
    {
        Backoff.Compute(5, Options, new FixedRandom(0.0)).ShouldBe(TimeSpan.Zero);
    }

    [Fact]
    public void Growth_is_capped_at_the_maximum()
    {
        Backoff.Compute(20, Options, new FixedRandom(1.0)).ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void A_large_attempt_does_not_overflow()
    {
        Backoff.Compute(1000, Options, new FixedRandom(1.0)).ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Jitter_stays_within_bounds_across_seeds()
    {
        var random = new Random(12345);

        for (var attempt = 1; attempt <= 12; attempt++)
        {
            var ceiling = Math.Min(30_000d, 500d * Math.Pow(2, attempt - 1));
            for (var i = 0; i < 50; i++)
            {
                var delay = Backoff.Compute(attempt, Options, random).TotalMilliseconds;
                delay.ShouldBeGreaterThanOrEqualTo(0);
                delay.ShouldBeLessThanOrEqualTo(ceiling + 0.001);
            }
        }
    }

    [Fact]
    public void Attempt_zero_is_rejected()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => Backoff.Compute(0, Options, new FixedRandom(0.5)));
    }
}
