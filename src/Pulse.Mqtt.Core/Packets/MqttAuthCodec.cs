using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttAuthPacket"/> (MQTT 5 only) to and from the wire.</summary>
public static class MqttAuthCodec
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    public static void Encode(IBufferWriter<byte> output, MqttAuthPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        var hasProperties = packet.AuthenticationMethod is not null
            || packet.AuthenticationData is not null
            || packet.ReasonString is not null
            || packet.UserProperties.Count > 0;

        // The reason code and properties may be omitted for a Success result with no properties.
        if (packet.ReasonCode != MqttReasonCode.Success || hasProperties)
        {
            writer.WriteByte((byte)packet.ReasonCode);

            if (hasProperties)
            {
                WriteProperties(body, packet);
            }
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Auth, 0x00, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes an AUTH packet from its body.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttAuthPacket Decode(ReadOnlySpan<byte> body)
    {
        var reasonCode = MqttReasonCode.Success;
        string? authenticationMethod = null;
        ReadOnlyMemory<byte>? authenticationData = null;
        string? reasonString = null;
        var userProperties = new List<MqttUserProperty>();

        if (!body.IsEmpty)
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
                        case MqttPropertyId.AuthenticationMethod:
                            authenticationMethod = properties.ReadString();
                            break;
                        case MqttPropertyId.AuthenticationData:
                            authenticationData = properties.ReadBinary().ToArray();
                            break;
                        case MqttPropertyId.ReasonString:
                            reasonString = properties.ReadString();
                            break;
                        case MqttPropertyId.UserProperty:
                            var (name, value) = properties.ReadStringPair();
                            userProperties.Add(new MqttUserProperty(name, value));
                            break;
                        default:
                            throw new MqttProtocolException($"Property {id} is not valid in AUTH.");
                    }
                }
            }
        }

        return new MqttAuthPacket
        {
            ReasonCode = reasonCode,
            AuthenticationMethod = authenticationMethod,
            AuthenticationData = authenticationData,
            ReasonString = reasonString,
            UserProperties = userProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttAuthPacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (packet.AuthenticationMethod is { } authenticationMethod)
        {
            props.WriteString(MqttPropertyId.AuthenticationMethod, authenticationMethod);
        }

        if (packet.AuthenticationData is { } authenticationData)
        {
            props.WriteBinary(MqttPropertyId.AuthenticationData, authenticationData.Span);
        }

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
