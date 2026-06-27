using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttPublishPacket"/> to and from the wire.</summary>
public static class MqttPublishCodec
{
    private const byte DupFlag = 0x08;
    private const byte RetainFlag = 0x01;

    private static readonly uint[] NoSubscriptionIdentifiers = [];
    private static readonly MqttUserProperty[] NoUserProperties = [];

    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    public static void Encode(IBufferWriter<byte> output, MqttPublishPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        var hasPacketId = packet.QualityOfService != MqttQualityOfService.AtMostOnce;
        if (hasPacketId && packet.PacketIdentifier is null)
        {
            throw new ArgumentException("A packet identifier is required for QoS greater than 0.", nameof(packet));
        }

        if (!hasPacketId && packet.PacketIdentifier is not null)
        {
            throw new ArgumentException("A packet identifier must not be set for QoS 0.", nameof(packet));
        }

        var isV5 = packet.ProtocolVersion == MqttProtocolVersion.V500;
        ValidateProtocolProperties(packet);
        if (!isV5 || !HasProperties(packet))
        {
            EncodeWithoutProperties(output, packet, hasPacketId, isV5);
            return;
        }

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteString(packet.Topic);
        if (hasPacketId)
        {
            writer.WriteUInt16(packet.PacketIdentifier!.Value);
        }

        WriteProperties(body, packet);

        if (!packet.Payload.IsEmpty)
        {
            var span = body.GetSpan(packet.Payload.Length);
            packet.Payload.Span.CopyTo(span);
            body.Advance(packet.Payload.Length);
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Publish, Flags(packet), body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    // The dominant shapes — no MQTT 5 properties, or only a topic alias (the hot pattern when
    // aliasing is enabled) — size the body up front and write it in one pass, with no
    // intermediate buffer and a single copy of the payload.
    private static void EncodeWithoutProperties(IBufferWriter<byte> output, MqttPublishPacket packet, bool hasPacketId, bool isV5)
    {
        var topicLength = Encoding.UTF8.GetByteCount(packet.Topic);
        if (topicLength > ushort.MaxValue)
        {
            throw new ArgumentException($"String exceeds the {ushort.MaxValue}-byte protocol limit.", nameof(packet));
        }

        // A topic alias is a 3-byte property: identifier 0x23 plus the two-byte alias.
        var propertiesLength = isV5 && packet.TopicAlias is not null ? 3 : 0;
        var remaining = 2 + topicLength
            + (hasPacketId ? 2 : 0)
            + (isV5 ? 1 + propertiesLength : 0)
            + packet.Payload.Length;

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Publish, Flags(packet), remaining));

        var span = output.GetSpan(remaining);
        BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)topicLength);
        var written = 2 + Encoding.UTF8.GetBytes(packet.Topic, span[2..]);

        if (hasPacketId)
        {
            BinaryPrimitives.WriteUInt16BigEndian(span[written..], packet.PacketIdentifier!.Value);
            written += 2;
        }

        if (isV5)
        {
            span[written++] = (byte)propertiesLength;
            if (packet.TopicAlias is { } topicAlias)
            {
                span[written++] = (byte)MqttPropertyId.TopicAlias;
                BinaryPrimitives.WriteUInt16BigEndian(span[written..], topicAlias);
                written += 2;
            }
        }

        packet.Payload.Span.CopyTo(span[written..]);
        output.Advance(remaining);
    }

    private static byte Flags(MqttPublishPacket packet) => (byte)(
        (packet.Dup ? DupFlag : 0)
        | ((byte)packet.QualityOfService << 1)
        | (packet.Retain ? RetainFlag : 0));

    // TopicAlias is absent here on purpose: the single-pass path encodes it directly.
    private static bool HasProperties(MqttPublishPacket packet) =>
        packet.PayloadFormatIndicator != MqttPayloadFormatIndicator.Unspecified
        || packet.MessageExpiryInterval is not null
        || packet.ResponseTopic is not null
        || packet.CorrelationData is not null
        || packet.ContentType is not null
        || packet.SubscriptionIdentifiers.Count > 0
        || packet.UserProperties.Count > 0;

    private static void ValidateProtocolProperties(MqttPublishPacket packet)
    {
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.PayloadFormatIndicator != MqttPayloadFormatIndicator.Unspecified,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.PayloadFormatIndicator));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.MessageExpiryInterval is not null,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.MessageExpiryInterval));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.TopicAlias is not null,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.TopicAlias));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ResponseTopic is not null,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.ResponseTopic));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.CorrelationData is not null,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.CorrelationData));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ContentType is not null,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.ContentType));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.SubscriptionIdentifiers.Count > 0,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.SubscriptionIdentifiers));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.UserProperties.Count > 0,
            nameof(MqttPublishPacket),
            nameof(MqttPublishPacket.UserProperties));
    }

    /// <summary>Decodes a PUBLISH packet using the fixed header's flags for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The packet is malformed.</exception>
    public static MqttPublishPacket Decode(MqttFixedHeader header, ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var dup = (header.Flags & DupFlag) != 0;
        var qos = (MqttQualityOfService)((header.Flags >> 1) & 0x03);
        if ((byte)qos == 3)
        {
            throw new MqttProtocolException("PUBLISH QoS value 3 is invalid.");
        }

        var retain = (header.Flags & RetainFlag) != 0;

        var reader = new MqttBufferReader(body);
        var topic = reader.ReadString();

        ushort? packetIdentifier = null;
        if (qos != MqttQualityOfService.AtMostOnce)
        {
            packetIdentifier = reader.ReadUInt16();
        }

        var payloadFormat = MqttPayloadFormatIndicator.Unspecified;
        uint? messageExpiryInterval = null;
        ushort? topicAlias = null;
        string? responseTopic = null;
        ReadOnlyMemory<byte>? correlationData = null;
        string? contentType = null;
        List<uint>? subscriptionIdentifiers = null;
        List<MqttUserProperty>? userProperties = null;

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
                    case MqttPropertyId.PayloadFormatIndicator:
                        payloadFormat = (MqttPayloadFormatIndicator)properties.ReadByte();
                        break;
                    case MqttPropertyId.MessageExpiryInterval:
                        messageExpiryInterval = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.TopicAlias:
                        topicAlias = properties.ReadUInt16();
                        break;
                    case MqttPropertyId.ResponseTopic:
                        responseTopic = properties.ReadString();
                        break;
                    case MqttPropertyId.CorrelationData:
                        correlationData = properties.ReadBinary().ToArray();
                        break;
                    case MqttPropertyId.ContentType:
                        contentType = properties.ReadString();
                        break;
                    case MqttPropertyId.SubscriptionIdentifier:
                        (subscriptionIdentifiers ??= []).Add(properties.ReadVarInt());
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        (userProperties ??= []).Add(new MqttUserProperty(name, value));
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in PUBLISH.");
                }
            }
        }

        var payload = reader.ReadSpan(reader.Remaining).ToArray();

        return new MqttPublishPacket
        {
            Topic = topic,
            Payload = payload,
            QualityOfService = qos,
            Dup = dup,
            Retain = retain,
            PacketIdentifier = packetIdentifier,
            ProtocolVersion = version,
            PayloadFormatIndicator = payloadFormat,
            MessageExpiryInterval = messageExpiryInterval,
            TopicAlias = topicAlias,
            ResponseTopic = responseTopic,
            CorrelationData = correlationData,
            ContentType = contentType,
            SubscriptionIdentifiers = subscriptionIdentifiers ?? (IReadOnlyList<uint>)NoSubscriptionIdentifiers,
            UserProperties = userProperties ?? (IReadOnlyList<MqttUserProperty>)NoUserProperties,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttPublishPacket packet)
    {
        using var scratch = new PooledBufferWriter();
        var props = new MqttPropertiesWriter(scratch);

        if (packet.PayloadFormatIndicator != MqttPayloadFormatIndicator.Unspecified)
        {
            props.WriteByte(MqttPropertyId.PayloadFormatIndicator, (byte)packet.PayloadFormatIndicator);
        }

        if (packet.MessageExpiryInterval is { } messageExpiryInterval)
        {
            props.WriteUInt32(MqttPropertyId.MessageExpiryInterval, messageExpiryInterval);
        }

        if (packet.TopicAlias is { } topicAlias)
        {
            props.WriteUInt16(MqttPropertyId.TopicAlias, topicAlias);
        }

        if (packet.ResponseTopic is { } responseTopic)
        {
            props.WriteString(MqttPropertyId.ResponseTopic, responseTopic);
        }

        if (packet.CorrelationData is { } correlationData)
        {
            props.WriteBinary(MqttPropertyId.CorrelationData, correlationData.Span);
        }

        if (packet.ContentType is { } contentType)
        {
            props.WriteString(MqttPropertyId.ContentType, contentType);
        }

        foreach (var subscriptionIdentifier in packet.SubscriptionIdentifiers)
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
