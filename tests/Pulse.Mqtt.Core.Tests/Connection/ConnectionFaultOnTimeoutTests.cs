using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

/// <summary>
/// An exchange that reaches the wire but is never acknowledged must fault the connection so the
/// supervisor reconnects — re-arming the send quota and packet-id allocator and (for a persistent
/// session) redelivering — instead of leaving a live connection that frees a slot the broker still
/// counts or recycles an id the broker still owns. Faulting completes <see cref="RawMqttClient.Completion"/>,
/// which is exactly the signal the resilient supervisor waits on to reconnect.
/// </summary>
public sealed class ConnectionFaultOnTimeoutTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class FixedTransportFactory(IMqttTransport transport) : IMqttTransportFactory
    {
        public ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken) => ValueTask.FromResult(transport);
    }

    private sealed class EchoAuthenticator : IMqttAuthenticator
    {
        public string Method => "TEST";

        public ValueTask<ReadOnlyMemory<byte>?> NextDataAsync(ReadOnlyMemory<byte>? challenge, CancellationToken cancellationToken) =>
            ValueTask.FromResult<ReadOnlyMemory<byte>?>(Encoding.UTF8.GetBytes("answer"));
    }

    [Fact]
    public async Task An_unacknowledged_publish_times_out_and_faults_the_connection()
    {
        var (client, broker, time, ct) = await ConnectedAsync();

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);
        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>(); // on the wire, never acknowledged

        await AdvanceUntilAsync(time, () => publishTask.IsCompleted);

        await Should.ThrowAsync<MqttException>(async () => await publishTask);
        await client.Completion.WaitAsync(SafetyTimeout);
        client.Completion.IsCompleted.ShouldBeTrue("an ack timeout must fault the connection so the supervisor reconnects");
    }

    [Fact]
    public async Task A_clean_start_publish_cancelled_after_it_is_sent_faults_the_connection()
    {
        var (client, broker, _, ct) = await ConnectedAsync();
        using var cancel = new CancellationTokenSource();

        var publishTask = client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce }, cancel.Token);
        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttPublishPacket>(); // on the wire

        await cancel.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(async () => await publishTask);
        await client.Completion.WaitAsync(SafetyTimeout);
        client.Completion.IsCompleted.ShouldBeTrue("a clean-start client cannot park the still-owned id, so the connection must fault");
    }

    [Fact]
    public async Task A_reauthentication_that_times_out_faults_the_connection()
    {
        var (client, broker, time, ct) = await ConnectedAsync(new EchoAuthenticator());

        var reauthTask = client.ReAuthenticateAsync(ct);
        (await broker.ReadPacketAsync(ct)).ShouldBeOfType<MqttAuthPacket>()
            .ReasonCode.ShouldBe(MqttReasonCode.ReAuthenticate); // on the wire, broker never concludes

        await AdvanceUntilAsync(time, () => reauthTask.IsCompleted);

        await Should.ThrowAsync<MqttException>(async () => await reauthTask);
        await client.Completion.WaitAsync(SafetyTimeout);
        client.Completion.IsCompleted.ShouldBeTrue("a re-auth timeout must fault the connection so a stale success cannot complete a later attempt");
    }

    private static async Task<(RawMqttClient Client, ScriptedBroker Broker, FakeTimeProvider Time, CancellationToken Ct)> ConnectedAsync(
        IMqttAuthenticator? authenticator = null)
    {
        var (clientTransport, serverTransport) = LoopbackTransport.CreatePair();
        var broker = new ScriptedBroker(serverTransport);
        var time = new FakeTimeProvider();
        var client = new RawMqttClient(
            new FixedTransportFactory(clientTransport),
            new RawMqttClientOptions { Authenticator = authenticator },
            time);

        var timeout = new CancellationTokenSource(SafetyTimeout);
        var connectTask = client.ConnectAsync(new MqttConnectPacket { ClientId = "c", KeepAliveSeconds = 0 }, timeout.Token);
        await broker.ReadPacketAsync(timeout.Token);
        await broker.SendAsync(new MqttConnAckPacket(), timeout.Token);
        await connectTask;

        return (client, broker, time, timeout.Token);
    }

    private static async Task AdvanceUntilAsync(FakeTimeProvider time, Func<bool> condition)
    {
        for (var i = 0; i < 400 && !condition(); i++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(5);
        }
    }
}
