using System.Buffers;

namespace Pulse.Mqtt.Codec;

/// <summary>Writes the length-prefixed MQTT 5 property section.</summary>
public static class MqttPropertySection
{
    /// <summary>Writes the property length (variable integer) followed by the property body.</summary>
    /// <param name="output">The destination buffer writer.</param>
    /// <param name="body">The already-encoded property bytes; may be empty.</param>
    public static void Write(IBufferWriter<byte> output, ReadOnlySpan<byte> body)
    {
        ArgumentNullException.ThrowIfNull(output);

        var writer = new MqttBufferWriter(output);
        writer.WriteVarInt((uint)body.Length);
        if (body.IsEmpty)
        {
            return;
        }

        var span = output.GetSpan(body.Length);
        body.CopyTo(span);
        output.Advance(body.Length);
    }

    /// <summary>Writes an empty property section (a single zero length byte).</summary>
    /// <param name="output">The destination buffer writer.</param>
    public static void WriteEmpty(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);

        var writer = new MqttBufferWriter(output);
        writer.WriteVarInt(0);
    }
}
