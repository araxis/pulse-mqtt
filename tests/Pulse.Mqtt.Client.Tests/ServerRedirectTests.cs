using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class ServerRedirectTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_disconnect_redirect_is_followed_instead_of_faulting()
    {
        var factory = new RedirectableSequencedFactory();
        await using var client = NewClient(factory, followRedirects: true, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.Inner.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Connected), timeout.Token);

        await broker1.SendAsync(
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ServerReference = "backup.example:1884",
            },
            timeout.Token);

        var broker2 = await factory.Inner.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Count(change => change.Current == ConnectionState.Connected) == 2, timeout.Token);

        factory.Targets.ShouldBe([("backup.example", 1884)]);
        transitions.Snapshot().ShouldNotContain(change => change.Current == ConnectionState.Faulted);

        await client.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task A_connack_redirect_retargets_the_next_attempt()
    {
        var factory = new RedirectableSequencedFactory();
        await using var client = NewClient(factory, followRedirects: true, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        // First broker rejects the CONNACK with a redirect; the retry must target the reference.
        var broker1 = await factory.Inner.NextBrokerAsync(timeout.Token);
        await broker1.ReadPacketAsync(timeout.Token); // the CONNECT
        await broker1.SendAsync(
            new MqttConnAckPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ServerReference = "moved.example",
            },
            timeout.Token);

        var broker2 = await factory.Inner.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Connected), timeout.Token);

        factory.Targets.ShouldBe([("moved.example", null)]);

        await client.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task A_connack_redirect_stays_terminal_when_the_option_is_off()
    {
        var factory = new RedirectableSequencedFactory();
        await using var client = NewClient(factory, followRedirects: false, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        var broker = await factory.Inner.NextBrokerAsync(timeout.Token);
        await broker.ReadPacketAsync(timeout.Token); // the CONNECT
        await broker.SendAsync(
            new MqttConnAckPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ServerReference = "moved.example",
            },
            timeout.Token);

        // With redirect-following off, a CONNACK-level redirect must fault rather than reconnect to
        // the same server forever. Before the fix the reason was not terminal on the CONNACK path
        // (only on the disconnect path), so the strategy retried the same endpoint indefinitely and
        // never faulted.
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Faulted), timeout.Token);
        factory.Targets.ShouldBeEmpty();
    }

    [Fact]
    public async Task The_hop_bound_turns_an_endless_redirect_chain_terminal()
    {
        var factory = new RedirectableSequencedFactory();
        await using var client = NewClient(factory, followRedirects: true, out var transitions, maxRedirects: 2);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        // Every broker accepts and then immediately redirects — the ping-pong misconfiguration
        // the bound exists for. Each hop connecting does not renew the budget; only a quiet
        // chain does. After two followed hops the third redirect faults.
        for (var hop = 0; hop < 3; hop++)
        {
            var broker = await factory.Inner.NextBrokerAsync(timeout.Token);
            await broker.AcceptConnectionAsync(timeout.Token);
            await transitions.WaitForAsync(
                changes => changes.Count(change => change.Current == ConnectionState.Connected) == hop + 1, timeout.Token);
            await broker.SendAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = MqttReasonCode.ServerMoved,
                    ServerReference = $"hop{hop}.example:2000",
                },
                timeout.Token);
        }

        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Faulted), timeout.Token);

        factory.Targets.Count.ShouldBe(2);
        transitions.Snapshot().Last().Reason.ShouldBe(MqttReasonCode.ServerMoved);

        await client.DisposeAsync();
    }

    [Fact]
    public async Task Redirects_stay_terminal_when_the_option_is_off()
    {
        var factory = new RedirectableSequencedFactory();
        await using var client = NewClient(factory, followRedirects: false, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.Inner.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Connected), timeout.Token);

        await broker.SendAsync(
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ServerReference = "backup.example:1884",
            },
            timeout.Token);

        await transitions.WaitForAsync(
            changes => changes.Any(change => change.Current == ConnectionState.Faulted), timeout.Token);
        factory.Targets.ShouldBeEmpty();
    }

    private static ResilientMqttClient NewClient(
        RedirectableSequencedFactory factory,
        bool followRedirects,
        out TransitionRecorder transitions,
        int maxRedirects = 8)
    {
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "redirect-tests", KeepAliveSeconds = 0 },
            FollowServerRedirects = followRedirects,
            MaxServerRedirects = maxRedirects,
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(10) },
        });

        var recorder = new TransitionRecorder();
        client.StateChanged += recorder.Record;
        transitions = recorder;
        return client;
    }

    /// <summary>
    /// A redirect-capable view over <see cref="SequencedTransportFactory"/>: records every
    /// retarget and keeps handing out brokers from the same scripted sequence.
    /// </summary>
    private sealed class RedirectableSequencedFactory : IRedirectableTransportFactory
    {
        public SequencedTransportFactory Inner { get; } = new();

        public List<(string Host, int? Port)> Targets { get; } = [];

        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) =>
            Inner.ConnectAsync(cancellationToken);

        public IMqttTransportFactory WithServer(string host, int? port)
        {
            Targets.Add((host, port));
            return this;
        }
    }
}
