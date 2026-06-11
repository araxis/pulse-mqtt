using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttDisconnectPacket"/> to and from the wire.</summary>
public static class MqttDisconnectCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    public static void Encode(IBufferWriter<byte> output, MqttDisconnectPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        if (packet.ProtocolVersion == MqttProtocolVersion.V500)
        {
            var hasProperties = packet.SessionExpiryInterval is not null
                || packet.ReasonString is not null
                || packet.ServerReference is not null
                || packet.UserProperties.Count > 0;

            // The reason code and properties may be omitted for a normal (Success) disconnect with no properties.
            if (packet.ReasonCode != MqttReasonCode.Success || hasProperties)
            {
                writer.WriteByte((byte)packet.ReasonCode);

                if (hasProperties)
                {
                    WriteProperties(body, packet);
                }
            }
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Disconnect, 0x00, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes a DISCONNECT packet from its body for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttDisconnectPacket Decode(ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var reasonCode = MqttReasonCode.Success;
        uint? sessionExpiryInterval = null;
        string? reasonString = null;
        string? serverReference = null;
        var userProperties = new List<MqttUserProperty>();

        if (version == MqttProtocolVersion.V500 && !body.IsEmpty)
        {
            var reader = new MqttBufferReader(body);
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
                        case MqttPropertyId.SessionExpiryInterval:
                            sessionExpiryInterval = properties.ReadUInt32();
                            break;
                        case MqttPropertyId.ReasonString:
                            reasonString = properties.ReadString();
                            break;
                        case MqttPropertyId.ServerReference:
                            serverReference = properties.ReadString();
                            break;
                        case MqttPropertyId.UserProperty:
                            var (name, value) = properties.ReadStringPair();
                            userProperties.Add(new MqttUserProperty(name, value));
                            break;
                        default:
                            throw new MqttProtocolException($"Property {id} is not valid in DISCONNECT.");
                    }
                }
            }
        }

        return new MqttDisconnectPacket
        {
            ReasonCode = reasonCode,
            ProtocolVersion = version,
            SessionExpiryInterval = sessionExpiryInterval,
            ReasonString = reasonString,
            ServerReference = serverReference,
            UserProperties = userProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttDisconnectPacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (packet.SessionExpiryInterval is { } sessionExpiryInterval)
        {
            props.WriteUInt32(MqttPropertyId.SessionExpiryInterval, sessionExpiryInterval);
        }

        if (packet.ReasonString is { } reasonString)
        {
            props.WriteString(MqttPropertyId.ReasonString, reasonString);
        }

        if (packet.ServerReference is { } serverReference)
        {
            props.WriteString(MqttPropertyId.ServerReference, serverReference);
        }

        foreach (var property in packet.UserProperties)
        {
            props.WriteStringPair(MqttPropertyId.UserProperty, property.Name, property.Value);
        }

        MqttPropertySection.Write(body, scratch.WrittenSpan);
    }
}
