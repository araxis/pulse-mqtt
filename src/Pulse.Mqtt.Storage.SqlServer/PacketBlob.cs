using System.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Storage.SqlServer;

internal static class PacketBlob
{
    public static byte[] Encode(MqttPublishPacket packet)
    {
        var writer = new ArrayBufferWriter<byte>();
        MqttPacketWriter.Write(writer, MqttPublishStorage.ToStorageForm(packet));
        return writer.WrittenMemory.ToArray();
    }

    public static MqttPublishPacket Decode(byte[] blob, MqttProtocolVersion version)
    {
        if (MqttFrameReader.TryReadFrame(blob, out var header, out var body, out _) != MqttFrameStatus.Complete)
        {
            throw new SqlServerStorageException("A stored packet blob is truncated or malformed.");
        }

        return MqttPacketDecoder.Decode(header, body, version) is MqttPublishPacket publish
            ? MqttPublishStorage.FromStorageForm(publish)
            : throw new SqlServerStorageException("A stored packet blob did not decode to a PUBLISH.");
    }
}
