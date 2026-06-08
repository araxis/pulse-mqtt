using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttFixedHeaderTests
{
    [Theory]
    [InlineData(MqttPacketType.Connect, (byte)0x00, (byte)0x10)]
    [InlineData(MqttPacketType.Publish, (byte)0x0B, (byte)0x3B)] // DUP+QoS1+RETAIN -> 0011 1011
    [InlineData(MqttPacketType.PubRel, (byte)0x02, (byte)0x62)]
    [InlineData(MqttPacketType.Auth, (byte)0x00, (byte)0xF0)]
    public void FirstByte_packs_type_and_flags(MqttPacketType type, byte flags, byte expected)
    {
        var header = new MqttFixedHeader(type, flags, 0);

        header.FirstByte.ShouldBe(expected);
    }
}
