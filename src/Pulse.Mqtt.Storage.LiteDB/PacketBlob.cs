using System.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Storage.LiteDB;

/// <summary>
/// Serializes a PUBLISH to its wire bytes for storage and back. Reusing the wire codec keeps the
/// stored form faithful to every MQTT 5 field; the protocol version is stored alongside so the
/// blob round-trips at the version it was written with.
/// </summary>
internal static class PacketBlob
{
    public static byte[] Encode(MqttPublishPacket packet)
    {
        // A QoS > 0 publish still in the offline queue has no identifier yet; store it with the
        // reserved placeholder so the wire codec (which requires one for QoS > 0) can encode it.
        var writer = new ArrayBufferWriter<byte>();
        MqttPacketWriter.Write(writer, MqttPublishStorage.ToStorageForm(packet));
        return writer.WrittenMemory.ToArray();
    }

    public static MqttPublishPacket Decode(byte[] blob, MqttProtocolVersion version)
    {
        if (MqttFrameReader.TryReadFrame(blob, out var header, out var body, out _) != MqttFrameStatus.Complete)
        {
            throw new LiteDbStorageException("A stored packet blob is truncated or malformed.");
        }

        return MqttPacketDecoder.Decode(header, body, version) is MqttPublishPacket publish
            ? MqttPublishStorage.FromStorageForm(publish)
            : throw new LiteDbStorageException("A stored packet blob did not decode to a PUBLISH.");
    }
}
