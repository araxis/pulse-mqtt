using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttPublishPacket"/> to and from the wire.</summary>
public static class MqttPublishCodec
{
    private const byte DupFlag = 0x08;
    private const byte RetainFlag = 0x01;

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

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteString(packet.Topic);
        if (hasPacketId)
        {
            writer.WriteUInt16(packet.PacketIdentifier!.Value);
        }

        if (packet.ProtocolVersion == MqttProtocolVersion.V500)
        {
            WriteProperties(body, packet);
        }

        if (!packet.Payload.IsEmpty)
        {
            var span = body.GetSpan(packet.Payload.Length);
            packet.Payload.Span.CopyTo(span);
            body.Advance(packet.Payload.Length);
        }

        var flags = (byte)(
            (packet.Dup ? DupFlag : 0)
            | ((byte)packet.QualityOfService << 1)
            | (packet.Retain ? RetainFlag : 0));

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.Publish, flags, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
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
        var subscriptionIdentifiers = new List<uint>();
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
                        subscriptionIdentifiers.Add(properties.ReadVarInt());
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
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
            SubscriptionIdentifiers = subscriptionIdentifiers,
            UserProperties = userProperties,
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
