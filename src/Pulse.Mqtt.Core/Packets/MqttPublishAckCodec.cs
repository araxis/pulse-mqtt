using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes the four publish acknowledgement packets that share one wire shape.</summary>
public static class MqttPublishAckCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    /// <exception cref="ArgumentException"><paramref name="packet"/> is not a publish acknowledgement type.</exception>
    public static void Encode(IBufferWriter<byte> output, MqttPublishAckPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        if (!IsAckType(packet.PacketType))
        {
            throw new ArgumentException($"{packet.PacketType} is not a publish acknowledgement.", nameof(packet));
        }

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteUInt16(packet.PacketIdentifier);

        if (packet.ProtocolVersion == MqttProtocolVersion.V500)
        {
            var hasProperties = packet.ReasonString is not null || packet.UserProperties.Count > 0;

            // The reason code and properties may be omitted when the result is Success with no properties.
            if (packet.ReasonCode != MqttReasonCode.Success || hasProperties)
            {
                writer.WriteByte((byte)packet.ReasonCode);

                if (hasProperties)
                {
                    WriteProperties(body, packet);
                }
            }
        }

        var flags = packet.PacketType == MqttPacketType.PubRel ? (byte)0x02 : (byte)0x00;
        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(packet.PacketType, flags, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes a publish acknowledgement from the fixed header (for the packet type) and body.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttPublishAckPacket Decode(MqttFixedHeader header, ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        if (!IsAckType(header.PacketType))
        {
            throw new MqttProtocolException($"{header.PacketType} is not a publish acknowledgement.");
        }

        var reader = new MqttBufferReader(body);
        var packetIdentifier = reader.ReadUInt16();

        var reasonCode = MqttReasonCode.Success;
        string? reasonString = null;
        var userProperties = new List<MqttUserProperty>();

        if (version == MqttProtocolVersion.V500 && reader.Remaining > 0)
        {
            reasonCode = (MqttReasonCode)reader.ReadByte();

            if (reader.Remaining > 0)
            {
                var length = reader.ReadVarInt();
                var section = reader.ReadSpan((int)length);
                var properties = new MqttPropertiesReader(section);
                while (properties.HasRemaining)
                {
                    var id = properties.ReadId();
                    switch (id)
                    {
                        case MqttPropertyId.ReasonString:
                            reasonString = properties.ReadString();
                            break;
                        case MqttPropertyId.UserProperty:
                            var (name, value) = properties.ReadStringPair();
                            userProperties.Add(new MqttUserProperty(name, value));
                            break;
                        default:
                            throw new MqttProtocolException($"Property {id} is not valid in {header.PacketType}.");
                    }
                }
            }
        }

        return new MqttPublishAckPacket
        {
            PacketType = header.PacketType,
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            ProtocolVersion = version,
            ReasonString = reasonString,
            UserProperties = userProperties,
        };
    }

    private static bool IsAckType(MqttPacketType packetType) => packetType
        is MqttPacketType.PubAck
        or MqttPacketType.PubRec
        or MqttPacketType.PubRel
        or MqttPacketType.PubComp;

    private static void WriteProperties(IBufferWriter<byte> body, MqttPublishAckPacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (packet.ReasonString is { } reasonString)
        {
            props.WriteString(MqttPropertyId.ReasonString, reasonString);
        }

        foreach (var property in packet.UserProperties)
        {
            props.WriteStringPair(MqttPropertyId.UserProperty, property.Name, property.Value);
        }

        MqttPropertySection.Write(body, scratch.WrittenSpan);
    }
}
