using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class ServerDisconnectTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_transient_broker_disconnect_reconnects_and_carries_the_reason()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker1.SendAsync(new MqttDisconnectPacket { ReasonCode = MqttReasonCode.ServerShuttingDown }, timeout.Token);

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        transitions.ShouldContain(change =>
            change.Current == ConnectionState.Reconnecting && change.Reason == MqttReasonCode.ServerShuttingDown);
    }

    [Fact]
    public async Task A_terminal_broker_disconnect_faults_instead_of_reconnecting()
    {
        var factory = new SequencedTransportFactory();
        await using var client = NewClient(factory, out var transitions);

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker.SendAsync(new MqttDisconnectPacket { ReasonCode = MqttReasonCode.SessionTakenOver }, timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Faulted, timeout.Token);
        factory.ConnectionsHandedOut.ShouldBe(1); // no reconnect attempt — the session has a new owner
        transitions.ShouldContain(change =>
            change.Current == ConnectionState.Faulted && change.Reason == MqttReasonCode.SessionTakenOver);
    }

    [Fact]
    public async Task The_lifecycle_sees_the_disconnect_details_before_the_fault()
    {
        var factory = new SequencedTransportFactory();
        var lifecycle = new RecordingLifecycle();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "down-context", KeepAliveSeconds = 0 },
            Lifecycle = lifecycle,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker.SendAsync(
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.UseAnotherServer,
                ReasonString = "rebalancing",
                ServerReference = "backup.example:1883",
            },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Faulted, timeout.Token);

        var context = lifecycle.LastDown.ShouldNotBeNull();
        context.Reason.ShouldBe(MqttReasonCode.UseAnotherServer);
        context.ReasonString.ShouldBe("rebalancing");
        context.ServerReference.ShouldBe("backup.example:1883");
        context.Error.ShouldBeOfType<MqttServerDisconnectedException>();
    }

    private sealed class RecordingLifecycle : IConnectionLifecycle
    {
        public IConnectionDownContext? LastDown { get; private set; }

        public ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask OnConnectionDownAsync(IConnectionDownContext context, CancellationToken cancellationToken)
        {
            LastDown = context;
            return ValueTask.CompletedTask;
        }
    }

    private static ResilientMqttClient NewClient(SequencedTransportFactory factory, out List<ConnectionStateChanged> transitions)
    {
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "disconnect-tests", KeepAliveSeconds = 0 },
        });

        var recorded = new List<ConnectionStateChanged>();
        client.StateChanged += recorded.Add;
        transitions = recorded;
        return client;
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
