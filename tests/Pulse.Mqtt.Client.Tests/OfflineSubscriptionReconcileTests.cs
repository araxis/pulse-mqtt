using System.Text;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Testing;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>
/// A subscription made while the client is offline must still reach the broker after the broker
/// resumes the persistent session (CONNACK SessionPresent = true). The lifecycle's full re-subscribe
/// runs only on a fresh session, so the offline delta is reconciled on connection-up.
/// </summary>
public sealed class OfflineSubscriptionReconcileTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task A_subscription_made_while_offline_reaches_the_broker_after_a_resumed_session()
    {
        await using var broker = new PulseMqttTestBroker { ResumeSessions = true };
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var topic = $"offline/{Guid.NewGuid():N}";

        var gated = new GatedReconnectFactory(broker);
        await using var subscriber = new ResilientMqttClient(gated, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "offsub", KeepAliveSeconds = 0, CleanStart = false },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(5), MaxDelay = TimeSpan.FromMilliseconds(50) },
        });
        await subscriber.ConnectAsync(timeout.Token);
        await WaitForStateAsync(subscriber, s => s == ConnectionState.Connected, timeout.Token);

        // Hold the client offline: block the next connect, then drop the live one.
        gated.BlockReconnect();
        await gated.KillAsync();
        await WaitForStateAsync(subscriber, s => s != ConnectionState.Connected, timeout.Token);

        // Subscribe while offline. It is stored but never sent; the resumed session must still learn it.
        var template = MqttRouteTemplate.Parse(topic);
        await subscriber.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], timeout.Token);
        await using var stream = subscriber.OpenRouteStream(template);

        // Let it reconnect; the broker resumes the persistent session (SessionPresent = true).
        gated.AllowReconnect();
        await WaitForStateAsync(subscriber, s => s == ConnectionState.Connected, timeout.Token);

        // Deliver to the offline-subscribed topic from the broker side.
        await broker.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = "resumed"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);

        // Before the fix the broker never had the subscription, so this read would time out.
        var routed = await stream.Reader.ReadAsync(timeout.Token);
        Encoding.UTF8.GetString(routed.Message.Payload.Span).ShouldBe("resumed");
    }

    [Fact]
    public async Task An_offline_subscription_survives_a_connection_dropped_mid_reconcile()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var topic = $"offline/{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = "offsub-drop",
                KeepAliveSeconds = 0,
                CleanStart = false,
                SessionExpiryInterval = 300,
            },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(5) },
        });

        // Subscribe while offline (never connected): recorded as a pending delta.
        var template = MqttRouteTemplate.Parse(topic);
        await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], timeout.Token);

        await client.ConnectAsync(timeout.Token);

        // Connection 1 resumes the session, so the reconcile sends the pending SUBSCRIBE — but the
        // broker drops the connection after reading it and before the SUBACK.
        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        var reconcile1 = (await broker1.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        reconcile1.TopicFilters.ShouldContain(f => f.Topic == topic);
        await broker1.DisposeAsync(); // dropped before the SUBACK — the reconcile round-trip fails

        // Connection 2 also resumes the session (so the lifecycle does not replay the stored set).
        // Before the fix the pending delta was cleared up front, so the broker never heard the
        // subscription again; after the fix the un-acked entry is still pending and re-sent here.
        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token, sessionPresent: true);
        var reconcile2 = (await broker2.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        reconcile2.TopicFilters.ShouldContain(f => f.Topic == topic);

        await broker2.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = reconcile2.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] },
            timeout.Token);
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, Func<ConnectionState, bool> predicate, CancellationToken cancellationToken)
    {
        while (!predicate(client.State))
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    // Holds the client offline across a deliberate gap: block the next connect, drop the current
    // transport, then release the block to let the supervisor reconnect.
    private sealed class GatedReconnectFactory(IMqttTransportFactory inner) : IMqttTransportFactory
    {
        private volatile IMqttTransport? _current;
        private volatile TaskCompletionSource? _gate;

        public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
        {
            if (_gate is { } gate)
            {
                await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            var transport = await inner.ConnectAsync(cancellationToken).ConfigureAwait(false);
            _current = transport;
            return transport;
        }

        public void BlockReconnect() => _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void AllowReconnect()
        {
            var gate = _gate;
            _gate = null;
            gate?.TrySetResult();
        }

        public ValueTask KillAsync() => _current?.DisposeAsync() ?? ValueTask.CompletedTask;
    }
}
