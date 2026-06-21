using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class EnhancedAuthTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    /// <summary>Echoes each challenge back with a step counter — enough to assert the exchange.</summary>
    private sealed class ScriptedAuthenticator : IMqttAuthenticator
    {
        public List<string?> Challenges { get; } = [];

        public string Method => "TEST-CHALLENGE";

        public ValueTask<ReadOnlyMemory<byte>?> NextDataAsync(ReadOnlyMemory<byte>? challenge, CancellationToken cancellationToken)
        {
            var text = challenge is { } data ? Encoding.UTF8.GetString(data.Span) : null;
            lock (Challenges)
            {
                Challenges.Add(text);
            }

            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(Encoding.UTF8.GetBytes($"answer-{Challenges.Count}"));
        }
    }

    [Fact]
    public async Task A_multi_step_exchange_completes_the_handshake()
    {
        var authenticator = new ScriptedAuthenticator();
        var (client, broker, timeout) = NewClient(authenticator);

        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);

        // CONNECT carries the method and the initial data.
        var connect = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttConnectPacket>();
        connect.AuthenticationMethod.ShouldBe("TEST-CHALLENGE");
        Encoding.UTF8.GetString(connect.AuthenticationData!.Value.Span).ShouldBe("answer-1");

        // Two challenge rounds, then the CONNACK.
        await broker.SendAsync(
            new MqttAuthPacket
            {
                ReasonCode = MqttReasonCode.ContinueAuthentication,
                AuthenticationMethod = "TEST-CHALLENGE",
                AuthenticationData = Encoding.UTF8.GetBytes("step-1"),
            },
            timeout.Token);
        var reply1 = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttAuthPacket>();
        reply1.ReasonCode.ShouldBe(MqttReasonCode.ContinueAuthentication);
        Encoding.UTF8.GetString(reply1.AuthenticationData!.Value.Span).ShouldBe("answer-2");

        await broker.SendAsync(
            new MqttAuthPacket
            {
                ReasonCode = MqttReasonCode.ContinueAuthentication,
                AuthenticationMethod = "TEST-CHALLENGE",
                AuthenticationData = Encoding.UTF8.GetBytes("step-2"),
            },
            timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttAuthPacket>();

        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        (await connectTask).ReasonCode.ShouldBe(MqttReasonCode.Success);

        authenticator.Challenges.ShouldBe([null, "step-1", "step-2"]);
    }

    [Fact]
    public async Task Enhanced_authentication_requires_mqtt_5()
    {
        var (client, _, timeout) = NewClient(new ScriptedAuthenticator());

        var thrown = await Should.ThrowAsync<NotSupportedException>(async () =>
            await client.ConnectAsync(
                new MqttConnectPacket
                {
                    ClientId = "c",
                    ProtocolVersion = MqttProtocolVersion.V311,
                },
                timeout.Token));

        thrown.Message.ShouldContain("MQTT 5.0");
    }

    [Fact]
    public async Task A_rejected_exchange_surfaces_the_connack()
    {
        var (client, broker, timeout) = NewClient(new ScriptedAuthenticator());

        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { ReasonCode = MqttReasonCode.NotAuthorized }, timeout.Token);

        (await connectTask).ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
    }

    [Fact]
    public async Task A_challenge_without_an_authenticator_is_a_protocol_error()
    {
        var (client, broker, timeout) = NewClient(authenticator: null);

        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(
            new MqttAuthPacket { ReasonCode = MqttReasonCode.ContinueAuthentication },
            timeout.Token);

        await Should.ThrowAsync<MqttProtocolException>(async () => await connectTask);
    }

    [Fact]
    public async Task Reauthentication_runs_a_full_exchange_on_the_live_connection()
    {
        var authenticator = new ScriptedAuthenticator();
        var (client, broker, timeout) = NewClient(authenticator);

        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        var reauthTask = client.ReAuthenticateAsync(timeout.Token);

        var start = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttAuthPacket>();
        start.ReasonCode.ShouldBe(MqttReasonCode.ReAuthenticate);
        start.AuthenticationMethod.ShouldBe("TEST-CHALLENGE");

        await broker.SendAsync(
            new MqttAuthPacket
            {
                ReasonCode = MqttReasonCode.ContinueAuthentication,
                AuthenticationMethod = "TEST-CHALLENGE",
                AuthenticationData = Encoding.UTF8.GetBytes("re-step"),
            },
            timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfType<MqttAuthPacket>()
            .ReasonCode.ShouldBe(MqttReasonCode.ContinueAuthentication);

        await broker.SendAsync(
            new MqttAuthPacket { ReasonCode = MqttReasonCode.Success, AuthenticationMethod = "TEST-CHALLENGE" },
            timeout.Token);

        await reauthTask;
        authenticator.Challenges.ShouldContain("re-step");
    }

    [Fact]
    public async Task Reauthentication_requires_an_authenticator()
    {
        var (client, broker, timeout) = NewClient(authenticator: null);

        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c" }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        await Should.ThrowAsync<InvalidOperationException>(() => client.ReAuthenticateAsync(timeout.Token));
    }

    [Fact]
    public async Task Reauthentication_requires_mqtt_5()
    {
        var (client, broker, timeout) = NewClient(authenticator: null);

        var connectTask = client.ConnectAsync(
            new MqttConnectPacket
            {
                ClientId = "c",
                ProtocolVersion = MqttProtocolVersion.V311,
            },
            timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket { ProtocolVersion = MqttProtocolVersion.V311 }, timeout.Token);
        await connectTask;

        var thrown = await Should.ThrowAsync<NotSupportedException>(() => client.ReAuthenticateAsync(timeout.Token));
        thrown.Message.ShouldContain("MQTT 5.0");
    }

    private static (RawMqttClient Client, ScriptedBroker Broker, CancellationTokenSource Timeout) NewClient(
        IMqttAuthenticator? authenticator)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var client = new RawMqttClient(
            new FixedTransportFactory(clientTransport),
            new RawMqttClientOptions { Authenticator = authenticator },
            new FakeTimeProvider());

        return (client, broker, new CancellationTokenSource(SafetyTimeout));
    }
}
