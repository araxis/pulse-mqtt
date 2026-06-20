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
        await subscriber.StartAsync(timeout.Token);
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
