using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttPropertiesWriterTests
{
    [Fact]
    public void Writes_section_that_reads_back_identically()
    {
        using var body = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(body);
        props.WriteUInt32(MqttPropertyId.SessionExpiryInterval, 3600);
        props.WriteString(MqttPropertyId.ContentType, "application/json");
        props.WriteStringPair(MqttPropertyId.UserProperty, "tenant", "acme");

        var output = new ArrayBufferWriter<byte>();
        MqttPropertySection.Write(output, body.WrittenSpan);

        var reader = new MqttBufferReader(output.WrittenSpan);
        var length = reader.ReadVarInt();
        var section = reader.ReadSpan((int)length);
        var read = new MqttPropertiesReader(section);

        read.ReadId().ShouldBe(MqttPropertyId.SessionExpiryInterval);
        read.ReadUInt32().ShouldBe(3600u);
        read.ReadId().ShouldBe(MqttPropertyId.ContentType);
        read.ReadString().ShouldBe("application/json");
        read.ReadId().ShouldBe(MqttPropertyId.UserProperty);
        var pair = read.ReadStringPair();
        pair.Name.ShouldBe("tenant");
        pair.Value.ShouldBe("acme");
        read.HasRemaining.ShouldBeFalse();
    }

    [Fact]
    public void WriteEmpty_writes_zero_length()
    {
        var output = new ArrayBufferWriter<byte>();

        MqttPropertySection.WriteEmpty(output);

        output.WrittenSpan.ToArray().ShouldBe(new byte[] { 0x00 });
    }
}
