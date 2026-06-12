using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class MqttConnectionTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task SendAsync_frames_a_packet_the_peer_can_read()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await connection.SendAsync(new MqttConnectPacket { ClientId = "engine" }, timeout.Token);

        var result = await serverTransport.Input.ReadAtLeastAsync(3, timeout.Token);
        var bytes = result.Buffer.ToArray();
        var status = MqttFrameReader.TryReadFrame(bytes, out var header, out var body, out _);

        status.ShouldBe(MqttFrameStatus.Complete);
        header.PacketType.ShouldBe(MqttPacketType.Connect);
        MqttConnectCodec.Decode(body).ClientId.ShouldBe("engine");

        await serverTransport.DisposeAsync();
    }

    [Fact]
    public async Task Two_packets_in_one_flush_both_arrive_inbound()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        MqttConnAckCodec.Encode(serverTransport.Output, new MqttConnAckPacket { SessionPresent = true });
        MqttPingCodec.WriteResponse(serverTransport.Output);
        await serverTransport.Output.FlushAsync(timeout.Token);

        (await connection.Inbound.ReadAsync(timeout.Token)).ShouldBeOfType<MqttConnAckPacket>().SessionPresent.ShouldBeTrue();
        (await connection.Inbound.ReadAsync(timeout.Token)).ShouldBeOfType<MqttPingRespPacket>();

        await serverTransport.DisposeAsync();
    }

    [Fact]
    public async Task A_packet_split_across_flushes_still_decodes()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var scratch = new ArrayBufferWriter<byte>();
        MqttConnAckCodec.Encode(scratch, new MqttConnAckPacket { ReasonCode = MqttReasonCode.Success });
        var encoded = scratch.WrittenSpan.ToArray();

        await serverTransport.Output.WriteAsync(encoded.AsMemory(0, 1), timeout.Token);
        await serverTransport.Output.WriteAsync(encoded.AsMemory(1), timeout.Token);

        (await connection.Inbound.ReadAsync(timeout.Token)).ShouldBeOfType<MqttConnAckPacket>();

        await serverTransport.DisposeAsync();
    }

    [Fact]
    public async Task Malformed_bytes_fault_the_inbound_channel()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await serverTransport.Output.WriteAsync(new byte[] { 0x00, 0x00 }, timeout.Token); // packet type 0 is invalid

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await connection.Inbound.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttProtocolException>();

        await serverTransport.DisposeAsync();
    }

    [Fact]
    public async Task Peer_close_completes_the_inbound_channel()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        await serverTransport.DisposeAsync();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await connection.Inbound.Completion.WaitAsync(timeout.Token);
        await connection.Completion.WaitAsync(timeout.Token);
    }

    [Fact]
    public async Task Close_mid_packet_faults_the_inbound_channel()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await serverTransport.Output.WriteAsync(new byte[] { 0x30, 0x05, 0xAA }, timeout.Token); // declares 5 body bytes, sends 1
        await serverTransport.DisposeAsync();

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await connection.Inbound.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttProtocolException>();
    }

    [Fact]
    public async Task Oversized_packet_faults_the_inbound_channel()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = new MqttConnection(clientTransport, new MqttConnectionOptions
        {
            MaxInboundPacketSize = 16,
        });
        connection.Start();

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        // PUBLISH declaring 100 body bytes: exceeds the 16-byte cap before any body arrives.
        await serverTransport.Output.WriteAsync(new byte[] { 0x30, 0x64 }, timeout.Token);

        var thrown = await Should.ThrowAsync<ChannelClosedException>(
            async () => await connection.Inbound.ReadAsync(timeout.Token));
        thrown.InnerException.ShouldBeOfType<MqttProtocolException>();

        await serverTransport.DisposeAsync();
    }

    [Fact]
    public async Task Start_twice_throws()
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        await using var connection = NewConnection(clientTransport);
        connection.Start();

        Should.Throw<InvalidOperationException>(connection.Start);

        await serverTransport.DisposeAsync();
    }

    private static MqttConnection NewConnection(IMqttTransport transport) =>
        new(transport, new MqttConnectionOptions());
}
