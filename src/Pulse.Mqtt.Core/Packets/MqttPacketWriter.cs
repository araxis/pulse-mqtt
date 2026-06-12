using System.Buffers;
using Pulse.Mqtt.Codec;

namespace Pulse.Mqtt.Packets;

/// <summary>Dispatches a control packet to the matching codec for encoding. Mirror of <see cref="MqttPacketDecoder"/>.</summary>
public static class MqttPacketWriter
{
    /// <summary>Encodes <paramref name="packet"/> into <paramref name="output"/>, fixed header included.</summary>
    /// <exception cref="ArgumentException">The packet type has no encoder.</exception>
    public static void Write(IBufferWriter<byte> output, MqttPacket packet)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(packet);

        switch (packet)
        {
            case MqttConnectPacket connect:
                MqttConnectCodec.Encode(output, connect);
                break;
            case MqttConnAckPacket connAck:
                MqttConnAckCodec.Encode(output, connAck);
                break;
            case MqttPublishPacket publish:
                MqttPublishCodec.Encode(output, publish);
                break;
            case MqttPublishAckPacket publishAck:
                MqttPublishAckCodec.Encode(output, publishAck);
                break;
            case MqttSubscribePacket subscribe:
                MqttSubscribeCodec.Encode(output, subscribe);
                break;
            case MqttSubAckPacket subAck:
                MqttSubAckCodec.Encode(output, subAck);
                break;
            case MqttUnsubscribePacket unsubscribe:
                MqttUnsubscribeCodec.Encode(output, unsubscribe);
                break;
            case MqttUnsubAckPacket unsubAck:
                MqttUnsubAckCodec.Encode(output, unsubAck);
                break;
            case MqttPingReqPacket:
                MqttPingCodec.WriteRequest(output);
                break;
            case MqttPingRespPacket:
                MqttPingCodec.WriteResponse(output);
                break;
            case MqttDisconnectPacket disconnect:
                MqttDisconnectCodec.Encode(output, disconnect);
                break;
            case MqttAuthPacket auth:
                MqttAuthCodec.Encode(output, auth);
                break;
            default:
                throw new ArgumentException($"No encoder for packet type {packet.GetType().Name}.", nameof(packet));
        }
    }
}
