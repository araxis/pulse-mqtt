namespace Pulse.Mqtt.Codec;

/// <summary>Outcome of attempting to read a control packet frame from a buffer.</summary>
public enum MqttFrameStatus
{
    /// <summary>A complete frame is available.</summary>
    Complete,

    /// <summary>More bytes are needed before the frame is complete.</summary>
    Incomplete,

    /// <summary>The frame violates the protocol (invalid packet type or reserved flag bits).</summary>
    Malformed,
}

/// <summary>
/// Reads MQTT control packet frames from a contiguous buffer. The receive loop appends bytes and
/// calls <see cref="TryReadFrame"/> until it reports <see cref="MqttFrameStatus.Complete"/>, then
/// advances past the consumed bytes and repeats. All operations are allocation-free.
/// </summary>
public static class MqttFrameReader
{
    /// <summary>Parses the fixed header at the start of <paramref name="buffer"/>.</summary>
    /// <param name="buffer">Bytes beginning at a packet boundary.</param>
    /// <param name="header">The parsed fixed header when the result is <see cref="MqttFrameStatus.Complete"/>.</param>
    /// <param name="headerLength">The fixed header's own length in bytes when complete.</param>
    public static MqttFrameStatus TryReadHeader(ReadOnlySpan<byte> buffer, out MqttFixedHeader header, out int headerLength)
    {
        header = default;
        headerLength = 0;

        if (buffer.IsEmpty)
        {
            return MqttFrameStatus.Incomplete;
        }

        var first = buffer[0];
        var packetType = (MqttPacketType)(first >> 4);
        var flags = (byte)(first & 0x0F);

        if (packetType is < MqttPacketType.Connect or > MqttPacketType.Auth || !FlagsValid(packetType, flags))
        {
            return MqttFrameStatus.Malformed;
        }

        var status = MqttVarInt.TryRead(buffer[1..], out var remainingLength, out var remainingLengthBytes);
        switch (status)
        {
            case MqttVarIntStatus.Incomplete:
                return MqttFrameStatus.Incomplete;
            case MqttVarIntStatus.Malformed:
                return MqttFrameStatus.Malformed;
            default:
                header = new MqttFixedHeader(packetType, flags, (int)remainingLength);
                headerLength = 1 + remainingLengthBytes;
                return MqttFrameStatus.Complete;
        }
    }

    /// <summary>Reads one complete frame: its fixed header, its body span, and the total bytes consumed.</summary>
    /// <param name="buffer">Bytes beginning at a packet boundary.</param>
    /// <param name="header">The parsed fixed header when the result is <see cref="MqttFrameStatus.Complete"/>.</param>
    /// <param name="body">The variable header and payload span when complete (a slice of <paramref name="buffer"/>; no copy).</param>
    /// <param name="consumed">Total bytes the complete frame occupies (fixed header + body) when complete; otherwise zero.</param>
    public static MqttFrameStatus TryReadFrame(
        ReadOnlySpan<byte> buffer,
        out MqttFixedHeader header,
        out ReadOnlySpan<byte> body,
        out int consumed)
    {
        body = default;
        consumed = 0;

        var status = TryReadHeader(buffer, out header, out var headerLength);
        if (status != MqttFrameStatus.Complete)
        {
            return status;
        }

        var total = headerLength + header.RemainingLength;
        if (buffer.Length < total)
        {
            return MqttFrameStatus.Incomplete;
        }

        body = buffer.Slice(headerLength, header.RemainingLength);
        consumed = total;
        return MqttFrameStatus.Complete;
    }

    private static bool FlagsValid(MqttPacketType packetType, byte flags) => packetType switch
    {
        // PUBLISH carries DUP/QoS/RETAIN in its flags, so any combination is structurally valid here.
        MqttPacketType.Publish => true,

        // PUBREL, SUBSCRIBE, and UNSUBSCRIBE require the reserved bit pattern 0b0010.
        MqttPacketType.PubRel or MqttPacketType.Subscribe or MqttPacketType.Unsubscribe => flags == 0x02,

        // All other packet types require flags of 0b0000.
        _ => flags == 0x00,
    };
}
