using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

/// <summary>
/// The Phase 1 Definition of Done: malformed or truncated input must never surface an uncontrolled
/// exception. The frame reader must not throw at all, and the packet decoder must only ever throw
/// <see cref="MqttProtocolException"/>.
/// </summary>
public sealed class MqttDecoderFuzzTests
{
    [Fact]
    public void Fully_random_buffers_never_throw_uncontrolled()
    {
        var random = new Random(20260608);
        for (var i = 0; i < 10_000; i++)
        {
            var buffer = new byte[random.Next(0, 48)];
            random.NextBytes(buffer);
            FeedThrough(buffer, NextVersion(random));
        }
    }

    [Fact]
    public void Framed_random_bodies_never_throw_uncontrolled()
    {
        var random = new Random(99);
        for (var i = 0; i < 10_000; i++)
        {
            var packetType = (byte)random.Next(1, 16);
            var flags = (byte)random.Next(0, 16);
            var bodyLength = random.Next(0, 40);

            var buffer = new byte[2 + bodyLength];
            buffer[0] = (byte)((packetType << 4) | flags);
            buffer[1] = (byte)bodyLength; // single-byte remaining length (< 128)
            random.NextBytes(buffer.AsSpan(2));

            FeedThrough(buffer, NextVersion(random));
        }
    }

    private static MqttProtocolVersion NextVersion(Random random) =>
        random.Next(2) == 0 ? MqttProtocolVersion.V311 : MqttProtocolVersion.V500;

    private static void FeedThrough(byte[] buffer, MqttProtocolVersion version)
    {
        // TryReadFrame must never throw — it reports status instead.
        var status = MqttFrameReader.TryReadFrame(buffer, out var header, out var body, out _);
        if (status != MqttFrameStatus.Complete)
        {
            return;
        }

        try
        {
            MqttPacketDecoder.Decode(header, body, version);
        }
        catch (MqttProtocolException)
        {
            // The only acceptable failure for malformed input. Any other exception propagates and fails the test.
        }
    }
}
