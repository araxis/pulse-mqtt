using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttPropertiesReaderTests
{
    [Fact]
    public void Reads_typed_properties_in_order()
    {
        // ServerKeepAlive (0x13) u16=30, then PayloadFormatIndicator (0x01) byte=1.
        byte[] section = [0x13, 0x00, 0x1E, 0x01, 0x01];

        var reader = new MqttPropertiesReader(section);

        reader.HasRemaining.ShouldBeTrue();
        reader.ReadId().ShouldBe(MqttPropertyId.ServerKeepAlive);
        reader.ReadUInt16().ShouldBe((ushort)30);
        reader.ReadId().ShouldBe(MqttPropertyId.PayloadFormatIndicator);
        reader.ReadByte().ShouldBe((byte)1);
        reader.HasRemaining.ShouldBeFalse();
    }

    [Fact]
    public void ReadId_throws_on_unknown_property()
    {
        byte[] section = [0x7F];

        Should.Throw<MqttProtocolException>(() =>
        {
            var reader = new MqttPropertiesReader(section);
            reader.ReadId();
        });
    }
}
