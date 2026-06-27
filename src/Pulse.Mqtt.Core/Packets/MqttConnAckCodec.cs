using System.Buffers;
using Pulse.Mqtt.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Encodes and decodes <see cref="MqttConnAckPacket"/> to and from the wire.</summary>
public static class MqttConnAckCodec
{
    private const byte SessionPresentFlag = 0x01;
    private const byte ReservedAckFlags = 0xFE;

    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    public static void Encode(IBufferWriter<byte> output, MqttConnAckPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        using var body = new PooledBufferWriter();
        var writer = new MqttBufferWriter(body);

        writer.WriteByte((byte)(packet.SessionPresent ? SessionPresentFlag : 0));
        writer.WriteByte((byte)packet.ReasonCode);

        ValidateProtocolProperties(packet);

        if (packet.ProtocolVersion == MqttProtocolVersion.V500)
        {
            WriteProperties(body, packet);
        }

        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.ConnAck, 0x00, body.WrittenCount));
        var destination = output.GetSpan(body.WrittenCount);
        body.WrittenSpan.CopyTo(destination);
        output.Advance(body.WrittenCount);
    }

    /// <summary>Decodes a CONNACK packet from its body for the negotiated <paramref name="version"/>.</summary>
    /// <exception cref="MqttProtocolException">The body is malformed.</exception>
    public static MqttConnAckPacket Decode(ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        var reader = new MqttBufferReader(body);

        var ackFlags = reader.ReadByte();
        if ((ackFlags & ReservedAckFlags) != 0)
        {
            throw new MqttProtocolException("CONNACK reserved acknowledge flags must be zero.");
        }

        var sessionPresent = (ackFlags & SessionPresentFlag) != 0;
        var reasonCode = (MqttReasonCode)reader.ReadByte();

        uint? sessionExpiryInterval = null;
        ushort? receiveMaximum = null;
        MqttQualityOfService? maximumQoS = null;
        bool? retainAvailable = null;
        uint? maximumPacketSize = null;
        string? assignedClientIdentifier = null;
        ushort? topicAliasMaximum = null;
        string? reasonString = null;
        var userProperties = new List<MqttUserProperty>();
        bool? wildcardSubscriptionAvailable = null;
        bool? subscriptionIdentifiersAvailable = null;
        bool? sharedSubscriptionAvailable = null;
        ushort? serverKeepAlive = null;
        string? responseInformation = null;
        string? serverReference = null;
        string? authenticationMethod = null;
        ReadOnlyMemory<byte>? authenticationData = null;

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
                    case MqttPropertyId.SessionExpiryInterval:
                        sessionExpiryInterval = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.ReceiveMaximum:
                        receiveMaximum = properties.ReadUInt16();
                        break;
                    case MqttPropertyId.MaximumQoS:
                        maximumQoS = (MqttQualityOfService)properties.ReadByte();
                        break;
                    case MqttPropertyId.RetainAvailable:
                        retainAvailable = properties.ReadByte() != 0;
                        break;
                    case MqttPropertyId.MaximumPacketSize:
                        maximumPacketSize = properties.ReadUInt32();
                        break;
                    case MqttPropertyId.AssignedClientIdentifier:
                        assignedClientIdentifier = properties.ReadString();
                        break;
                    case MqttPropertyId.TopicAliasMaximum:
                        topicAliasMaximum = properties.ReadUInt16();
                        break;
                    case MqttPropertyId.ReasonString:
                        reasonString = properties.ReadString();
                        break;
                    case MqttPropertyId.UserProperty:
                        var (name, value) = properties.ReadStringPair();
                        userProperties.Add(new MqttUserProperty(name, value));
                        break;
                    case MqttPropertyId.WildcardSubscriptionAvailable:
                        wildcardSubscriptionAvailable = properties.ReadByte() != 0;
                        break;
                    case MqttPropertyId.SubscriptionIdentifierAvailable:
                        subscriptionIdentifiersAvailable = properties.ReadByte() != 0;
                        break;
                    case MqttPropertyId.SharedSubscriptionAvailable:
                        sharedSubscriptionAvailable = properties.ReadByte() != 0;
                        break;
                    case MqttPropertyId.ServerKeepAlive:
                        serverKeepAlive = properties.ReadUInt16();
                        break;
                    case MqttPropertyId.ResponseInformation:
                        responseInformation = properties.ReadString();
                        break;
                    case MqttPropertyId.ServerReference:
                        serverReference = properties.ReadString();
                        break;
                    case MqttPropertyId.AuthenticationMethod:
                        authenticationMethod = properties.ReadString();
                        break;
                    case MqttPropertyId.AuthenticationData:
                        authenticationData = properties.ReadBinary().ToArray();
                        break;
                    default:
                        throw new MqttProtocolException($"Property {id} is not valid in CONNACK.");
                }
            }
        }

        return new MqttConnAckPacket
        {
            SessionPresent = sessionPresent,
            ReasonCode = reasonCode,
            ProtocolVersion = version,
            SessionExpiryInterval = sessionExpiryInterval,
            ReceiveMaximum = receiveMaximum,
            MaximumQoS = maximumQoS,
            RetainAvailable = retainAvailable,
            MaximumPacketSize = maximumPacketSize,
            AssignedClientIdentifier = assignedClientIdentifier,
            TopicAliasMaximum = topicAliasMaximum,
            ReasonString = reasonString,
            UserProperties = userProperties,
            WildcardSubscriptionAvailable = wildcardSubscriptionAvailable,
            SubscriptionIdentifiersAvailable = subscriptionIdentifiersAvailable,
            SharedSubscriptionAvailable = sharedSubscriptionAvailable,
            ServerKeepAlive = serverKeepAlive,
            ResponseInformation = responseInformation,
            ServerReference = serverReference,
            AuthenticationMethod = authenticationMethod,
            AuthenticationData = authenticationData,
        };
    }

    private static void WriteProperties(IBufferWriter<byte> body, MqttConnAckPacket packet)
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

        if (packet.MaximumQoS is { } maximumQoS)
        {
            props.WriteByte(MqttPropertyId.MaximumQoS, (byte)maximumQoS);
        }

        if (packet.RetainAvailable is { } retainAvailable)
        {
            props.WriteByte(MqttPropertyId.RetainAvailable, (byte)(retainAvailable ? 1 : 0));
        }

        if (packet.MaximumPacketSize is { } maximumPacketSize)
        {
            props.WriteUInt32(MqttPropertyId.MaximumPacketSize, maximumPacketSize);
        }

        if (packet.AssignedClientIdentifier is { } assignedClientIdentifier)
        {
            props.WriteString(MqttPropertyId.AssignedClientIdentifier, assignedClientIdentifier);
        }

        if (packet.TopicAliasMaximum is { } topicAliasMaximum)
        {
            props.WriteUInt16(MqttPropertyId.TopicAliasMaximum, topicAliasMaximum);
        }

        if (packet.ReasonString is { } reasonString)
        {
            props.WriteString(MqttPropertyId.ReasonString, reasonString);
        }

        if (packet.WildcardSubscriptionAvailable is { } wildcard)
        {
            props.WriteByte(MqttPropertyId.WildcardSubscriptionAvailable, (byte)(wildcard ? 1 : 0));
        }

        if (packet.SubscriptionIdentifiersAvailable is { } subscriptionIdentifiers)
        {
            props.WriteByte(MqttPropertyId.SubscriptionIdentifierAvailable, (byte)(subscriptionIdentifiers ? 1 : 0));
        }

        if (packet.SharedSubscriptionAvailable is { } shared)
        {
            props.WriteByte(MqttPropertyId.SharedSubscriptionAvailable, (byte)(shared ? 1 : 0));
        }

        if (packet.ServerKeepAlive is { } serverKeepAlive)
        {
            props.WriteUInt16(MqttPropertyId.ServerKeepAlive, serverKeepAlive);
        }

        if (packet.ResponseInformation is { } responseInformation)
        {
            props.WriteString(MqttPropertyId.ResponseInformation, responseInformation);
        }

        if (packet.ServerReference is { } serverReference)
        {
            props.WriteString(MqttPropertyId.ServerReference, serverReference);
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

    private static void ValidateProtocolProperties(MqttConnAckPacket packet)
    {
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.SessionExpiryInterval is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.SessionExpiryInterval));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ReceiveMaximum is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.ReceiveMaximum));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.MaximumQoS is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.MaximumQoS));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.RetainAvailable is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.RetainAvailable));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.MaximumPacketSize is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.MaximumPacketSize));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.AssignedClientIdentifier is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.AssignedClientIdentifier));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.TopicAliasMaximum is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.TopicAliasMaximum));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ReasonString is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.ReasonString));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.UserProperties.Count > 0,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.UserProperties));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.WildcardSubscriptionAvailable is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.WildcardSubscriptionAvailable));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.SubscriptionIdentifiersAvailable is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.SubscriptionIdentifiersAvailable));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.SharedSubscriptionAvailable is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.SharedSubscriptionAvailable));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ServerKeepAlive is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.ServerKeepAlive));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ResponseInformation is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.ResponseInformation));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.ServerReference is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.ServerReference));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.AuthenticationMethod is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.AuthenticationMethod));
        MqttProtocolCompatibility.ThrowIfMqtt5PropertyUsedWithV311(
            packet.ProtocolVersion,
            packet.AuthenticationData is not null,
            nameof(MqttConnAckPacket),
            nameof(MqttConnAckPacket.AuthenticationData));
    }
}
