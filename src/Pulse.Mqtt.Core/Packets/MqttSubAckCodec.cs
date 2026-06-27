using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttSubAckPacket"/> to and from the wire.</summary>
public static class MqttSubAckCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    /// <exception cref="ArgumentException"><paramref name="packet"/> has no reason codes.</exception>
    public static void Encode(IBufferWriter<byte> output, MqttSubAckPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        if (packet.ReasonCodes.Count == 0)
        {
            throw new ArgumentException("A SUBACK must contain at least one reason code.", nameof(packet));
        }

        ValidateProtocolProperties(packet);

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteUInt16(packet.PacketIdentifier);

        if (packet.ProtocolVersion == MqttProtocolVersion.V500)
        {
            WriteProperties(body, packet);
        }

        foreach (var reasonCode in packet.ReasonCodes)
        {
            writer.WriteByte((byte)reasonCode);
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.SubAck, 0x00, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes a SUBACK packet from its body for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttSubAckPacket Decode(ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var reader = new MqttBufferReader(body);
        var packetIdentifier = reader.ReadUInt16();

        string? reasonString = null;
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
                    case MqttPropertyId.ReasonString:
                        reasonString = properties.ReadString();
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in SUBACK.");
                }
            }
        }

        var reasonCodes = new List<MqttReasonCode>();
        while (reader.Remaining > 0)
        {
            reasonCodes.Add((MqttReasonCode)reader.ReadByte());
        }

        if (reasonCodes.Count == 0)
        {
            throw new MqttProtocolException("A SUBACK must contain at least one reason code.");
        }

        return new MqttSubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCodes = reasonCodes,
            ProtocolVersion = version,
            ReasonString = reasonString,
            UserProperties = userProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttSubAckPacket packet)
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

    private static void ValidateProtocolProperties(MqttSubAckPacket packet)
    {
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ReasonString is not null,
            nameof(MqttSubAckPacket),
            nameof(MqttSubAckPacket.ReasonString));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.UserProperties.Count > 0,
            nameof(MqttSubAckPacket),
            nameof(MqttSubAckPacket.UserProperties));
    }
}
