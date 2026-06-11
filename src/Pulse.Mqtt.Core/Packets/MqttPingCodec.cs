using System.Buffers;
using Pulse.Mqtt.Codec;

namespace Pulse.Mqtt.Packets;

/// <summary>Writes the two keep-alive packets, PINGREQ and PINGRESP, which carry no body.</summary>
public static class MqttPingCodec
{
    /// <summary>Writes a PINGREQ packet (fixed header only).</summary>
    public static void WriteRequest(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.PingReq, 0x00, 0));
    }

    /// <summary>Writes a PINGRESP packet (fixed header only).</summary>
    public static void WriteResponse(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        MqttFrameWriter.WriteHeader(output, new MqttFixedHeader(MqttPacketType.PingResp, 0x00, 0));
    }

    /// <summary>Verifies a keep-alive packet has the empty body the protocol requires.</summary>
    /// <exception cref="MqttProtocolException">The body is not empty.</exception>
    public static void EnsureEmptyBody(ReadOnlySpan<byte> body)
    {
        if (!body.IsEmpty)
        {
            throw new MqttProtocolException("A PINGREQ/PINGRESP packet must have an empty body.");
        }
    }
}
