using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttUnsubAckPacket"/> to and from the wire.</summary>
public static class MqttUnsubAckCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    /// <exception cref="ArgumentException">A v5 packet has no reason codes.</exception>
    public static void Encode(IBufferWriter<byte> output, MqttUnsubAckPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        var isV5 = packet.ProtocolVersion == MqttProtocolVersion.V500;
        if (isV5 && packet.ReasonCodes.Count == 0)
        {
            throw new ArgumentException("A v5 UNSUBACK must contain at least one reason code.", nameof(packet));
        }

        ValidateProtocolProperties(packet);

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteUInt16(packet.PacketIdentifier);

        // MQTT 3.1.1 UNSUBACK has no variable header beyond the packet identifier and no payload.
        if (isV5)
        {
            WriteProperties(body, packet);
            foreach (var reasonCode in packet.ReasonCodes)
            {
                writer.WriteByte((byte)reasonCode);
            }
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.UnsubAck, 0x00, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes an UNSUBACK packet from its body for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttUnsubAckPacket Decode(ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var reader = new MqttBufferReader(body);
        var packetIdentifier = reader.ReadUInt16();

        string? reasonString = null;
        var userProperties = new List<MqttUserProperty>();
        var reasonCodes = new List<MqttReasonCode>();

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
                        throw new MqttProtocolException($"Property {id} is not valid in UNSUBACK.");
                }
            }

            while (reader.Remaining > 0)
            {
                reasonCodes.Add((MqttReasonCode)reader.ReadByte());
            }
        }

        return new MqttUnsubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCodes = reasonCodes,
            ProtocolVersion = version,
            ReasonString = reasonString,
            UserProperties = userProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttUnsubAckPacket packet)
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

    private static void ValidateProtocolProperties(MqttUnsubAckPacket packet)
    {
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ReasonCodes.Count > 0,
            nameof(MqttUnsubAckPacket),
            nameof(MqttUnsubAckPacket.ReasonCodes));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ReasonString is not null,
            nameof(MqttUnsubAckPacket),
            nameof(MqttUnsubAckPacket.ReasonString));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.UserProperties.Count > 0,
            nameof(MqttUnsubAckPacket),
            nameof(MqttUnsubAckPacket.UserProperties));
    }
}
