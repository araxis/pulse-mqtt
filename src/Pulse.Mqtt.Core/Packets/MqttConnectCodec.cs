using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttConnectPacket"/> to and from the wire.</summary>
public static class MqttConnectCodec
{
    private const string ProtocolName = "MQTT";

    // Connect flag bit masks.
    private const byte UsernameFlag = 0x80;
    private const byte PasswordFlag = 0x40;
    private const byte WillRetainFlag = 0x20;
    private const byte WillFlag = 0x04;
    private const byte CleanStartFlag = 0x02;
    private const byte ReservedFlag = 0x01;

    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    public static void Encode(IBufferWriter<byte> output, MqttConnectPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        // Variable header.
        writer.WriteString(ProtocolName);
        writer.WriteByte((byte)packet.ProtocolVersion);
        writer.WriteByte(BuildConnectFlags(packet));
        writer.WriteUInt16(packet.KeepAliveSeconds);

        var isV5 = packet.ProtocolVersion == MqttProtocolVersion.V500;
        if (isV5)
        {
            WriteConnectProperties(body, packet);
        }

        // Payload.
        writer.WriteString(packet.ClientId);

        if (packet.Will is { } will)
        {
            if (isV5)
            {
                WriteWillProperties(body, will);
            }

            writer.WriteString(will.Topic);
            writer.WriteBinary(will.Payload.Span);
        }

        if (packet.Username is { } username)
        {
            writer.WriteString(username);
        }

        if (packet.Password is { } password)
        {
            writer.WriteBinary(password.Span);
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Connect, 0x00, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes a CONNECT packet from its body (variable header + payload).</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttConnectPacket Decode(ReadOnlySpan<byte> body)
    {
        var reader = new MqttBufferReader(body);

        var protocolName = reader.ReadString();
        if (protocolName != ProtocolName)
        {
            throw new MqttProtocolException($"Unexpected protocol name '{protocolName}'.");
        }

        var version = reader.ReadByte() switch
        {
            4 => MqttProtocolVersion.V311,
            5 => MqttProtocolVersion.V500,
            var other => throw new MqttProtocolException($"Unsupported protocol level {other}."),
        };

        var flags = reader.ReadByte();
        if ((flags & ReservedFlag) != 0)
        {
            throw new MqttProtocolException("CONNECT reserved flag must be zero.");
        }

        var hasUsername = (flags & UsernameFlag) != 0;
        var hasPassword = (flags & PasswordFlag) != 0;
        var willRetain = (flags & WillRetainFlag) != 0;
        var willQos = (MqttQualityOfService)((flags >> 3) & 0x03);
        var hasWill = (flags & WillFlag) != 0;
        var cleanStart = (flags & CleanStartFlag) != 0;

        if (!hasWill && (willRetain || willQos != MqttQualityOfService.AtMostOnce))
        {
            throw new MqttProtocolException("Will flags must be zero when no will is present.");
        }

        var keepAlive = reader.ReadUInt16();
        var isV5 = version == MqttProtocolVersion.V500;

        uint? sessionExpiry = null;
        ushort? receiveMaximum = null;
        uint? maximumPacketSize = null;
        ushort? topicAliasMaximum = null;
        var requestResponseInformation = false;
        var requestProblemInformation = true;
        string? authenticationMethod = null;
        ReadOnlyMemory<byte>? authenticationData = null;
        var userProperties = new List<MqttUserProperty>();

        if (isV5)
        {
            var section = ReadPropertySection(ref reader);
            var properties = new MqttPropertiesReader(section);
            while (properties.HasRemaining)
            {
                var id = properties.ReadId();
                switch (id)
                {
                    case MqttPropertyId.SessionExpiryInterval:
                        sessionExpiry = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.ReceiveMaximum:
                        receiveMaximum = properties.ReadUInt16();
                        break;
                    case MqttPropertyId.MaximumPacketSize:
                        maximumPacketSize = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.TopicAliasMaximum:
                        topicAliasMaximum = properties.ReadUInt16();
                        break;
                    case MqttPropertyId.RequestResponseInformation:
                        requestResponseInformation = properties.ReadByte() != 0;
                        break;
                    case MqttPropertyId.RequestProblemInformation:
                        requestProblemInformation = properties.ReadByte() != 0;
                        break;
                    case MqttPropertyId.AuthenticationMethod:
                        authenticationMethod = properties.ReadString();
                        break;
                    case MqttPropertyId.AuthenticationData:
                        authenticationData = properties.ReadBinary().ToArray();
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in CONNECT.");
                }
            }
        }

        var clientId = reader.ReadString();

        MqttWillMessage? will = null;
        if (hasWill)
        {
            will = ReadWill(ref reader, isV5, willQos, willRetain);
        }

        var username = hasUsername ? reader.ReadString() : null;

        ReadOnlyMemory<byte>? password = null;
        if (hasPassword)
        {
            password = reader.ReadBinary().ToArray();
        }

        return new MqttConnectPacket
        {
            ClientId = clientId,
            ProtocolVersion = version,
            CleanStart = cleanStart,
            KeepAliveSeconds = keepAlive,
            Username = username,
            Password = password,
            Will = will,
            SessionExpiryInterval = sessionExpiry,
            ReceiveMaximum = receiveMaximum,
            MaximumPacketSize = maximumPacketSize,
            TopicAliasMaximum = topicAliasMaximum,
            RequestResponseInformation = requestResponseInformation,
            RequestProblemInformation = requestProblemInformation,
            AuthenticationMethod = authenticationMethod,
            AuthenticationData = authenticationData,
            UserProperties = userProperties,
        };
    }

    private static MqttWillMessage ReadWill(ref MqttBufferReader reader, bool isV5, MqttQualityOfService qos, bool retain)
    {
        uint? delayInterval = null;
        var payloadFormat = MqttPayloadFormatIndicator.Unspecified;
        uint? messageExpiry = null;
        string? contentType = null;
        string? responseTopic = null;
        ReadOnlyMemory<byte>? correlationData = null;
        var userProperties = new List<MqttUserProperty>();

        if (isV5)
        {
            var section = ReadPropertySection(ref reader);
            var properties = new MqttPropertiesReader(section);
            while (properties.HasRemaining)
            {
                var id = properties.ReadId();
                switch (id)
                {
                    case MqttPropertyId.WillDelayInterval:
                        delayInterval = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.PayloadFormatIndicator:
                        payloadFormat = (MqttPayloadFormatIndicator)properties.ReadByte();
                        break;
                    case MqttPropertyId.MessageExpiryInterval:
                        messageExpiry = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.ContentType:
                        contentType = properties.ReadString();
                        break;
                    case MqttPropertyId.ResponseTopic:
                        responseTopic = properties.ReadString();
                        break;
                    case MqttPropertyId.CorrelationData:
                        correlationData = properties.ReadBinary().ToArray();
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in a will message.");
                }
            }
        }

        var topic = reader.ReadString();
        var payload = reader.ReadBinary().ToArray();

        return new MqttWillMessage(topic)
        {
            Payload = payload,
            QualityOfService = qos,
            Retain = retain,
            DelayInterval = delayInterval,
            PayloadFormatIndicator = payloadFormat,
            MessageExpiryInterval = messageExpiry,
            ContentType = contentType,
            ResponseTopic = responseTopic,
            CorrelationData = correlationData,
            UserProperties = userProperties,
        };
    }

    private static ReadOnlySpan<byte> ReadPropertySection(ref MqttBufferReader reader)
    {
        var length = reader.ReadVarInt();
        return reader.ReadSpan((int)length);
    }

    private static byte BuildConnectFlags(MqttConnectPacket packet)
    {
        byte flags = 0;
        if (packet.CleanStart)
        {
            flags |= CleanStartFlag;
        }

        if (packet.Will is { } will)
        {
            flags |= WillFlag;
            flags |= (byte)((byte)will.QualityOfService << 3);
            if (will.Retain)
            {
                flags |= WillRetainFlag;
            }
        }

        if (packet.Username is not null)
        {
            flags |= UsernameFlag;
        }

        if (packet.Password is not null)
        {
            flags |= PasswordFlag;
        }

        return flags;
    }

    private static void WriteConnectProperties(IBufferWriter<byte> body, MqttConnectPacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (packet.SessionExpiryInterval is { } sessionExpiry)
        {
            props.WriteUInt32(MqttPropertyId.SessionExpiryInterval, sessionExpiry);
        }

        if (packet.ReceiveMaximum is { } receiveMaximum)
        {
            props.WriteUInt16(MqttPropertyId.ReceiveMaximum, receiveMaximum);
        }

        if (packet.MaximumPacketSize is { } maximumPacketSize)
        {
            props.WriteUInt32(MqttPropertyId.MaximumPacketSize, maximumPacketSize);
        }

        if (packet.TopicAliasMaximum is { } topicAliasMaximum)
        {
            props.WriteUInt16(MqttPropertyId.TopicAliasMaximum, topicAliasMaximum);
        }

        if (packet.RequestResponseInformation)
        {
            props.WriteByte(MqttPropertyId.RequestResponseInformation, 1);
        }

        if (!packet.RequestProblemInformation)
        {
            props.WriteByte(MqttPropertyId.RequestProblemInformation, 0);
        }

        if (packet.AuthenticationMethod is { } authenticationMethod)
        {
            props.WriteString(MqttPropertyId.AuthenticationMethod, authenticationMethod);
        }

        if (packet.AuthenticationData is { } authenticationData)
        {
            props.WriteBinary(MqttPropertyId.AuthenticationData, authenticationData.Span);
        }

        foreach (var property in packet.UserProperties)
        {
            props.WriteStringPair(MqttPropertyId.UserProperty, property.Name, property.Value);
        }

        MqttPropertySection.Write(body, scratch.WrittenSpan);
    }

    private static void WriteWillProperties(IBufferWriter<byte> body, MqttWillMessage will)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (will.DelayInterval is { } delayInterval)
        {
            props.WriteUInt32(MqttPropertyId.WillDelayInterval, delayInterval);
        }

        if (will.PayloadFormatIndicator != MqttPayloadFormatIndicator.Unspecified)
        {
            props.WriteByte(MqttPropertyId.PayloadFormatIndicator, (byte)will.PayloadFormatIndicator);
        }

        if (will.MessageExpiryInterval is { } messageExpiry)
        {
            props.WriteUInt32(MqttPropertyId.MessageExpiryInterval, messageExpiry);
        }

        if (will.ContentType is { } contentType)
        {
            props.WriteString(MqttPropertyId.ContentType, contentType);
        }

        if (will.ResponseTopic is { } responseTopic)
        {
            props.WriteString(MqttPropertyId.ResponseTopic, responseTopic);
        }

        if (will.CorrelationData is { } correlationData)
        {
            props.WriteBinary(MqttPropertyId.CorrelationData, correlationData.Span);
        }

        foreach (var property in will.UserProperties)
        {
            props.WriteStringPair(MqttPropertyId.UserProperty, property.Name, property.Value);
        }

        MqttPropertySection.Write(body, scratch.WrittenSpan);
    }
}
