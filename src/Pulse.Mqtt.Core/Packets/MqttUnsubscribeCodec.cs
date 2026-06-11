using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttUnsubscribePacket"/> to and from the wire.</summary>
public static class MqttUnsubscribeCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    /// <exception cref="ArgumentException"><paramref name="packet"/> has no topic filters.</exception>
    public static void Encode(IBufferWriter<byte> output, MqttUnsubscribePacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        if (packet.TopicFilters.Count == 0)
        {
            throw new ArgumentException("An UNSUBSCRIBE must contain at least one topic filter.", nameof(packet));
        }

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteUInt16(packet.PacketIdentifier);

        if (packet.ProtocolVersion == MqttProtocolVersion.V500)
        {
            WriteProperties(body, packet);
        }

        foreach (var topicFilter in packet.TopicFilters)
        {
            writer.WriteString(topicFilter);
        }

        // UNSUBSCRIBE requires the reserved fixed-header flags 0b0010.
        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Unsubscribe, 0x02, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes an UNSUBSCRIBE packet from its body for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttUnsubscribePacket Decode(ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var reader = new MqttBufferReader(body);
        var packetIdentifier = reader.ReadUInt16();

        var userProperties = new List<MqttUserProperty>();

        if (version == MqttProtocolVersion.V500)
        {
            var length = reader.ReadVarInt();
            var section = reader.ReadSpan((int)length);
            var properties = new MqttPropertiesReader(section);
            while (properties.HasRemaining)
            {
                var id = properties.ReadId();
                switch (id)
                {
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in UNSUBSCRIBE.");
                }
            }
        }

        var topicFilters = new List<string>();
        while (reader.Remaining > 0)
        {
            topicFilters.Add(reader.ReadString());
        }

        if (topicFilters.Count == 0)
        {
            throw new MqttProtocolException("An UNSUBSCRIBE must contain at least one topic filter.");
        }

        return new MqttUnsubscribePacket
        {
            PacketIdentifier = packetIdentifier,
            TopicFilters = topicFilters,
            ProtocolVersion = version,
            UserProperties = userProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttUnsubscribePacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        foreach (var property in packet.UserProperties)
        {
            props.WriteStringPair(MqttPropertyId.UserProperty, property.Name, property.Value);
        }

        MqttPropertySection.Write(body, scratch.WrittenSpan);
    }
}
