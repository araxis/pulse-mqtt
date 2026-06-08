using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttBufferReaderSpanTests
{
    [Fact]
    public void ReadSpan_returns_slice_and_advances()
    {
        byte[] data = [0x01, 0x02, 0x03, 0x04];
        var reader = new MqttBufferReader(data);

        reader.ReadByte();
        var span = reader.ReadSpan(2);

        span.ToArray().ShouldBe(new byte[] { 0x02, 0x03 });
        reader.Remaining.ShouldBe(1);
    }

    [Fact]
    public void ReadSpan_throws_when_insufficient_bytes()
    {
        Should.Throw<MqttProtocolException>(() =>
        {
            var reader = new MqttBufferReader(new byte[] { 0x01 });
            reader.ReadSpan(2);
        });
    }
}
