using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class PresenceTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_static_will_rides_every_connect()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "willed", KeepAliveSeconds = 0 },
            Will = new MqttWillMessage("status/willed") { Payload = "offline"u8.ToArray(), Retain = true },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);

        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        var connect1 = (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        connect1.Will.ShouldNotBeNull().Topic.ShouldBe("status/willed");
        connect1.Will!.Retain.ShouldBeTrue();
        await broker1.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        // The same will rides the reconnect.
        await broker1.DisposeAsync();
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        var connect2 = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        connect2.Will.ShouldNotBeNull().Topic.ShouldBe("status/willed");
    }

    [Fact]
    public async Task A_will_factory_produces_a_fresh_will_per_attempt()
    {
        var calls = 0;
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "willed", KeepAliveSeconds = 0 },
            WillFactory = _ =>
            {
                var n = Interlocked.Increment(ref calls);
                return ValueTask.FromResult(new MqttWillMessage($"status/attempt/{n}"));
            },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);

        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>()
            .Will.ShouldNotBeNull().Topic.ShouldBe("status/attempt/1");
        await broker1.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker1.DisposeAsync();
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>()
            .Will.ShouldNotBeNull().Topic.ShouldBe("status/attempt/2");
    }

    [Fact]
    public async Task The_birth_publishes_after_resubscription_and_before_the_queue_flush()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "born", KeepAliveSeconds = 0 },
            Birth = new MqttPublishPacket { Topic = "status/born", Payload = "online"u8.ToArray(), Retain = true },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);

        // A durable subscription and a queued publish set up the ordering assertion.
        await client.SubscribeAsync([new MqttTopicFilter("cmd/born")], timeout.Token);
        (await client.PublishAsync(
            new MqttPublishPacket { Topic = "queued/1", Payload = new byte[] { 1 }, QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token)).Disposition.ShouldBe(PublishDisposition.Queued);

        await client.StartAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        // Wire order: SUBSCRIBE (re-subscription) -> birth PUBLISH -> queued PUBLISH.
        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.Success] },
            timeout.Token);

        var birth = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        birth.Topic.ShouldBe("status/born");
        birth.Retain.ShouldBeTrue();
        client.State.ShouldNotBe(ConnectionState.Connected, "the state must not be Connected before the birth went out");

        var queued = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        queued.Topic.ShouldBe("queued/1");
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = queued.PacketIdentifier!.Value },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task A_birth_factory_sees_the_attempt_counter()
    {
        var attempts = new List<int>();
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "born", KeepAliveSeconds = 0 },
            BirthFactory = (attempt, _) =>
            {
                lock (attempts)
                {
                    attempts.Add(attempt);
                }

                return ValueTask.FromResult(new MqttPublishPacket { Topic = $"status/up/{attempt}" });
            },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);

        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>().Topic.ShouldBe("status/up/0");
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker1.DisposeAsync();
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>().Topic.ShouldBe("status/up/1");
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        lock (attempts)
        {
            attempts.ShouldBe([0, 1]);
        }
    }

    [Fact]
    public async Task A_failing_birth_fails_the_connection_up_by_default()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "born", KeepAliveSeconds = 0 },
            BirthFactory = (_, _) => throw new InvalidOperationException("no birth today"),
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);

        // Each connection-up fails at the birth and the cycle retries with a new connection.
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);

        factory.ConnectionsHandedOut.ShouldBeGreaterThanOrEqualTo(2);
        client.State.ShouldNotBe(ConnectionState.Connected);
    }

    [Fact]
    public async Task A_failing_birth_can_log_and_continue()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "born", KeepAliveSeconds = 0 },
            Birth = new MqttPublishPacket
            {
                Topic = "status/born",
                Payload = new byte[64],
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            BirthFailure = BirthFailurePolicy.LogAndContinue,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);

        // The broker's tiny maximum packet size makes the birth publish fail client-side;
        // with LogAndContinue the connection still comes up.
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token, maximumPacketSize: 16);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
