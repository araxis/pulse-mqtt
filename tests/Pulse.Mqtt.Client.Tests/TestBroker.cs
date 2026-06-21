using System.Buffers;
using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>The broker side of one loopback connection: reads client packets, sends scripted replies.</summary>
internal sealed class TestBroker
{
    private readonly IMqttTransport _transport;
    private byte[] _pending = [];
    private MqttProtocolVersion _protocolVersion = MqttProtocolVersion.V500;

    public TestBroker(IMqttTransport serverTransport)
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

    public async Task AcceptConnectionAsync(
        CancellationToken cancellationToken,
        bool sessionPresent = false,
        uint? maximumPacketSize = null)
    {
        var connect = (await ReadPacketAsync(cancellationToken)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        _protocolVersion = connect.ProtocolVersion;
        await SendAsync(
            new MqttConnAckPacket
            {
                ProtocolVersion = _protocolVersion,
                SessionPresent = sessionPresent,
                MaximumPacketSize = maximumPacketSize,
            },
            cancellationToken);
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

        packet = MqttPacketDecoder.Decode(header, body, _protocolVersion);
        _pending = _pending[consumed..];
        return true;
    }
}

internal static class TestBrokerAssertions
{
    public static T ShouldBeOfTypeOrThrow<T>(this MqttPacket packet) =>
        packet is T typed ? typed : throw new InvalidOperationException($"Expected {typeof(T).Name} but received {packet.GetType().Name}.");
}

/// <summary>
/// Hands out one loopback connection per connect attempt and exposes the broker side of each, in
/// order, so tests script every session of a reconnecting client.
/// </summary>
internal sealed class SequencedTransportFactory : IMqttTransportFactory
{
    private readonly Channel<TestBroker> _brokers = Channel.CreateUnbounded<TestBroker>();

    public int ConnectionsHandedOut { get; private set; }

    public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
    {
        var (client, server) = LoopbackTransport.CreatePair();
        _brokers.Writer.TryWrite(new TestBroker(server));
        ConnectionsHandedOut++;
        return ValueTask.FromResult(client);
    }

    public ValueTask<TestBroker> NextBrokerAsync(CancellationToken cancellationToken) =>
        _brokers.Reader.ReadAsync(cancellationToken);
}
