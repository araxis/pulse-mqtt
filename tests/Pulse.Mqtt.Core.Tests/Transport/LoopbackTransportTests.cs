using System.Buffers;
using System.IO.Pipelines;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Transport;

public sealed class LoopbackTransportTests
{
    [Fact]
    public async Task Packet_written_to_client_is_read_on_server()
    {
        var (client, server) = LoopbackTransport.CreatePair();

        MqttConnectCodec.Encode(client.Output, new MqttConnectPacket { ClientId = "device-1" });
        await client.Output.FlushAsync();

        var packet = await ReadOnePacketAsync(server.Input);

        packet.ShouldBeOfType<MqttConnectPacket>().ClientId.ShouldBe("device-1");

        await client.DisposeAsync();
        await server.DisposeAsync();
    }

    [Fact]
    public async Task Packet_written_to_server_is_read_on_client()
    {
        var (client, server) = LoopbackTransport.CreatePair();

        MqttConnAckCodec.Encode(server.Output, new MqttConnAckPacket { SessionPresent = true });
        await server.Output.FlushAsync();

        var packet = await ReadOnePacketAsync(client.Input);

        packet.ShouldBeOfType<MqttConnAckPacket>().SessionPresent.ShouldBeTrue();

        await client.DisposeAsync();
        await server.DisposeAsync();
    }

    private static async Task<MqttPacket> ReadOnePacketAsync(PipeReader reader)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (true)
        {
            var result = await reader.ReadAsync(timeout.Token);
            var buffer = result.Buffer;
            var bytes = buffer.ToArray();

            var status = MqttFrameReader.TryReadFrame(bytes, out var header, out var body, out var consumed);
            if (status == MqttFrameStatus.Complete)
            {
                var packet = MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
                reader.AdvanceTo(buffer.GetPosition(consumed));
                return packet;
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted)
            {
                throw new InvalidOperationException("The transport completed before a full packet arrived.");
            }
        }
    }
}
