using Pulse.Mqtt.Buffers;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Buffers;

public sealed class PooledBufferWriterTests
{
    [Fact]
    public void Accumulates_written_bytes()
    {
        using var writer = new PooledBufferWriter(8);

        var span = writer.GetSpan(3);
        span[0] = 1;
        span[1] = 2;
        span[2] = 3;
        writer.Advance(3);

        writer.WrittenCount.ShouldBe(3);
        writer.WrittenSpan.ToArray().ShouldBe(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void Grows_beyond_initial_capacity()
    {
        using var writer = new PooledBufferWriter(4);

        for (var i = 0; i < 100; i++)
        {
            var span = writer.GetSpan(1);
            span[0] = (byte)i;
            writer.Advance(1);
        }

        writer.WrittenCount.ShouldBe(100);
        writer.WrittenSpan[99].ShouldBe((byte)99);
    }

    [Fact]
    public void Reset_clears_written_count()
    {
        using var writer = new PooledBufferWriter();
        writer.GetSpan(2);
        writer.Advance(2);

        writer.Reset();

        writer.WrittenCount.ShouldBe(0);
    }

    [Fact]
    public void Advance_past_end_throws()
    {
        using var writer = new PooledBufferWriter(4);
        writer.GetSpan(2);

        Should.Throw<InvalidOperationException>(() => writer.Advance(1_000));
    }
}
