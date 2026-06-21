using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>Dispatches a framed control packet to the matching codec based on its packet type.</summary>
public static class MqttPacketDecoder
{
    /// <summary>Decodes the packet described by <paramref name="header"/> from its <paramref name="body"/>.</summary>
    /// <param name="header">The fixed header produced by the frame reader.</param>
    /// <param name="body">The variable header and payload bytes.</param>
    /// <param name="version">The negotiated protocol version.</param>
    /// <exception cref="MqttProtocolException">The packet type is unsupported or the body is malformed.</exception>
    public static MqttPacket Decode(MqttFixedHeader header, ReadOnlySpan<byte> body, MqttProtocolVersion version)
    {
        switch (header.PacketType)
        {
            case MqttPacketType.Connect:
                return MqttConnectCodec.Decode(body);
            case MqttPacketType.ConnAck:
                return MqttConnAckCodec.Decode(body, version);
            case MqttPacketType.Publish:
                return MqttPublishCodec.Decode(header, body, version);
            case MqttPacketType.PubAck:
            case MqttPacketType.PubRec:
            case MqttPacketType.PubRel:
            case MqttPacketType.PubComp:
                return MqttPublishAckCodec.Decode(header, body, version);
            case MqttPacketType.Subscribe:
                return MqttSubscribeCodec.Decode(body, version);
            case MqttPacketType.SubAck:
                return MqttSubAckCodec.Decode(body, version);
            case MqttPacketType.Unsubscribe:
                return MqttUnsubscribeCodec.Decode(body, version);
            case MqttPacketType.UnsubAck:
                return MqttUnsubAckCodec.Decode(body, version);
            case MqttPacketType.PingReq:
                MqttPingCodec.EnsureEmptyBody(body);
                return new MqttPingReqPacket();
            case MqttPacketType.PingResp:
                MqttPingCodec.EnsureEmptyBody(body);
                return new MqttPingRespPacket();
            case MqttPacketType.Disconnect:
                return MqttDisconnectCodec.Decode(body, version);
            case MqttPacketType.Auth:
                if (version != MqttProtocolVersion.V500)
                {
                    throw new MqttProtocolException("AUTH packets are valid only in MQTT 5.0.");
                }

                return MqttAuthCodec.Decode(body);
            default:
                throw new MqttProtocolException($"Unsupported packet type {header.PacketType}.");
        }
    }
}
