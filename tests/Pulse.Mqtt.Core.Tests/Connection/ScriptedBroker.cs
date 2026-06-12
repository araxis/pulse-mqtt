using System.Buffers;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Core.Tests.Connection;

/// <summary>The broker side of a loopback link: reads client packets and sends scripted replies.</summary>
internal sealed class ScriptedBroker
{
    private readonly IMqttTransport _transport;
    private byte[] _pending = [];

    public ScriptedBroker(IMqttTransport serverTransport)
    {
        _transport = serverTransport;
    }

    public async Task<MqttPacket> ReadPacketAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryTakeBufferedPacket(out var buffered))
            {
                return buffered;
            }

            var result = await _transport.Input.ReadAsync(cancellationToken);
            _pending = [.. _pending, .. result.Buffer.ToArray()];
            _transport.Input.AdvanceTo(result.Buffer.End);

            if (TryTakeBufferedPacket(out var packet))
            {
                return packet;
            }

            if (result.IsCompleted)
            {
                throw new InvalidOperationException("The client closed before a full packet arrived.");
            }
        }
    }

    public async Task SendAsync(MqttPacket packet, CancellationToken cancellationToken)
    {
        MqttPacketWriter.Write(_transport.Output, packet);
        await _transport.Output.FlushAsync(cancellationToken);
    }

    public ValueTask DisposeAsync() => _transport.DisposeAsync();

    private bool TryTakeBufferedPacket(out MqttPacket packet)
    {
        packet = null!;
        var status = MqttFrameReader.TryReadFrame(_pending, out var header, out var body, out var consumed);
        if (status != MqttFrameStatus.Complete)
        {
            return false;
        }

        packet = MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
        _pending = _pending[consumed..];
        return true;
    }
}
