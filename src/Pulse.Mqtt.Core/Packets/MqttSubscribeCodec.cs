using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttSubscribePacket"/> to and from the wire.</summary>
public static class MqttSubscribeCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    /// <exception cref="ArgumentException"><paramref name="packet"/> has no topic filters.</exception>
    public static void Encode(IBufferWriter<byte> output, MqttSubscribePacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        if (packet.TopicFilters.Count == 0)
        {
            throw new ArgumentException("A SUBSCRIBE must contain at least one topic filter.", nameof(packet));
        }

        var isV5 = packet.ProtocolVersion == MqttProtocolVersion.V500;

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteUInt16(packet.PacketIdentifier);

        if (isV5)
        {
            WriteProperties(body, packet);
        }

        foreach (var filter in packet.TopicFilters)
        {
            writer.WriteString(filter.Topic);
            writer.WriteByte(isV5 ? filter.ToSubscriptionOptions() : (byte)filter.MaximumQualityOfService);
        }

        // SUBSCRIBE requires the reserved fixed-header flags 0b0010.
        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Subscribe, 0x02, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes a SUBSCRIBE packet from its body for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttSubscribePacket Decode(ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var reader = new MqttBufferReader(body);
        var packetIdentifier = reader.ReadUInt16();

        uint? subscriptionIdentifier = null;
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
                    case MqttPropertyId.SubscriptionIdentifier:
                        subscriptionIdentifier = properties.ReadVarInt();
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in SUBSCRIBE.");
                }
            }
        }

        var filters = new List<MqttTopicFilter>();
        while (reader.Remaining > 0)
        {
            var topic = reader.ReadString();
            var options = reader.ReadByte();
            filters.Add(MqttTopicFilter.FromSubscriptionOptions(topic, options));
        }

        if (filters.Count == 0)
        {
            throw new MqttProtocolException("A SUBSCRIBE must contain at least one topic filter.");
        }

        return new MqttSubscribePacket
        {
            PacketIdentifier = packetIdentifier,
            TopicFilters = filters,
            ProtocolVersion = version,
            SubscriptionIdentifier = subscriptionIdentifier,
            UserProperties = userProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttSubscribePacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (packet.SubscriptionIdentifier is { } subscriptionIdentifier)
        {
            props.WriteVarInt(MqttPropertyId.SubscriptionIdentifier, subscriptionIdentifier);
        }

        foreach (var property in packet.UserProperties)
        {
            props.WriteStringPair(MqttPropertyId.UserProperty, property.Name, property.Value);
        }

        MqttPropertySection.Write(body, scratch.WrittenSpan);
    }
}
