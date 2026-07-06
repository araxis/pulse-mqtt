using System.Buffers;
using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Endpoints.Tests;

internal sealed class WireTestBroker
{
    private readonly IMqttTransport _transport;
    private byte[] _pending = [];
    private MqttProtocolVersion _protocolVersion = MqttProtocolVersion.V500;

    public WireTestBroker(IMqttTransport serverTransport)
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

    public async Task AcceptConnectionAsync(CancellationToken cancellationToken)
    {
        var connect = (await ReadPacketAsync(cancellationToken)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        _protocolVersion = connect.ProtocolVersion;
        await SendAsync(
            new MqttConnAckPacket { ProtocolVersion = _protocolVersion },
            cancellationToken);
    }

    private bool TryTakeBufferedPacket(out MqttPacket packet)
    {
        packet = null!;
        var status = MqttFrameReader.TryReadFrame(_pending, out var header, out var body, out var consumed);
        if (status != MqttFrameStatus.Complete)
        {
            return false;
        }

        packet = MqttPacketDecoder.Decode(header, body, _protocolVersion);
        _pending = _pending[consumed..];
        return true;
    }
}

internal sealed class WireTransportFactory : IMqttTransportFactory
{
    private readonly Channel<WireTestBroker> _brokers = Channel.CreateUnbounded<WireTestBroker>();

    public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
    {
        var (client, server) = LoopbackTransport.CreatePair();
        _brokers.Writer.TryWrite(new WireTestBroker(server));
        return ValueTask.FromResult(client);
    }

    public ValueTask<WireTestBroker> NextBrokerAsync(CancellationToken cancellationToken) =>
        _brokers.Reader.ReadAsync(cancellationToken);
}

internal static class WireTestBrokerAssertions
{
    public static T ShouldBeOfTypeOrThrow<T>(this MqttPacket packet) =>
        packet is T typed ? typed : throw new InvalidOperationException($"Expected {typeof(T).Name} but received {packet.GetType().Name}.");
}
