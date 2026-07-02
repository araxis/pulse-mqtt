using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;

namespace Pulse.Mqtt.IntegrationTests.Brokers;

/// <summary>
/// The bounded chaos invariant, transport-agnostic: a publisher on a persistent session loses its
/// connection at random under sustained QoS 1 load; session preservation plus the offline queue and
/// automatic reconnect must deliver every message regardless of when the kills land. Runs against
/// whatever transport the broker's <see cref="IMqttBroker.CreateTransport"/> provides.
/// </summary>
public static class ChaosScenario
{
    private const int MessageCount = 30;
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(90);

    public static async Task RandomDisconnectsLoseNoQos1MessagesAsync(IMqttBroker broker)
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        var topic = $"chaos/{Guid.NewGuid():N}";

        // A stable durable subscriber collects deliveries, deduplicated by sequence number.
        await using var subscriber = new RawMqttClient(broker.CreateTransport());
        await subscriber.ConnectAsync(
            new MqttConnectPacket { ClientId = $"chaos-sub-{Guid.NewGuid():N}", KeepAliveSeconds = 30 },
            timeout.Token);
        await subscriber.SubscribeAsync(
            [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            timeout.Token);

        var received = new ConcurrentDictionary<int, byte>();
        var collector = Task.Run(async () =>
        {
            await foreach (var message in subscriber.Messages.ReadAllAsync(timeout.Token))
            {
                received[int.Parse(Encoding.UTF8.GetString(message.Payload.Span), CultureInfo.InvariantCulture)] = 0;
            }
        }, timeout.Token);

        // The publisher keeps a persistent session so the broker preserves its in-flight state
        // across the kills; a tight backoff keeps reconnects fast under the churn.
        var killable = new KillableTransportFactory(broker.CreateTransport());
        await using var publisher = new ResilientMqttClient(killable, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = $"chaos-pub-{Guid.NewGuid():N}",
                KeepAliveSeconds = 30,
                CleanStart = false,
                SessionExpiryInterval = 300,
            },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(50), MaxDelay = TimeSpan.FromMilliseconds(500) },
        });
        await publisher.ConnectAsync(timeout.Token);
        await WaitConnectedAsync(publisher, timeout.Token);

        // Kill the connection at random while publishing is in flight.
        using var chaos = new CancellationTokenSource();
        var random = new Random(20240613);
        var chaosLoop = Task.Run(async () =>
        {
            while (!chaos.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(random.Next(60, 180), chaos.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                await killable.KillAsync();
            }
        }, timeout.Token);

        // Sustained load: every QoS 1 publish either delivers, queues, or is held in flight.
        for (var sequence = 0; sequence < MessageCount; sequence++)
        {
            await publisher.PublishAsync(
                new MqttPublishPacket
                {
                    Topic = topic,
                    Payload = Encoding.UTF8.GetBytes(sequence.ToString(CultureInfo.InvariantCulture)),
                    QualityOfService = MqttQualityOfService.AtLeastOnce,
                },
                timeout.Token);
            await Task.Delay(20, timeout.Token);
        }

        // Stop the chaos and let the session resume and the queue drain.
        await chaos.CancelAsync();
        await chaosLoop;
        await WaitConnectedAsync(publisher, timeout.Token);

        using var settle = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (received.Count < MessageCount && !settle.IsCancellationRequested)
        {
            await Task.Delay(100, timeout.Token);
        }

        var missing = Enumerable.Range(0, MessageCount).Where(n => !received.ContainsKey(n)).ToList();
        missing.ShouldBeEmpty($"publisher state={publisher.State}, received={received.Count}/{MessageCount}");
        publisher.State.ShouldBe(ConnectionState.Connected);

        await publisher.DisconnectAsync(timeout.Token);
        await subscriber.DisconnectAsync(timeout.Token);
    }

    private static async Task WaitConnectedAsync(ResilientMqttClient client, CancellationToken cancellationToken)
    {
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(10, cancellationToken);
        }
    }

    private sealed class KillableTransportFactory(IMqttTransportFactory inner) : IMqttTransportFactory
    {
        private volatile IMqttTransport? _current;

        public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
        {
            var transport = await inner.ConnectAsync(cancellationToken).ConfigureAwait(false);
            _current = transport;
            return transport;
        }

        public ValueTask KillAsync() => _current?.DisposeAsync() ?? ValueTask.CompletedTask;
    }
}
