using System.Globalization;
using System.Text;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

/// <summary>
/// The soak harness: sustained QoS 1 traffic through endless random disconnects, for a duration set
/// by the <c>PULSE_SOAK_DURATION</c> environment variable (a <see cref="TimeSpan"/> string; default
/// 30 s for a smoke check; set to <c>1.00:00:00</c> — one day — for the real run, since
/// <c>24:00:00</c> parses as 24 <em>days</em>). It asserts zero lost messages, that reconnect always recovers,
/// and that the managed heap does not grow without bound. Tagged <c>Soak</c> so it stays out of the
/// normal CI run; invoke it with <c>dotnet test --filter "Category=Soak"</c> against a broker, and
/// restart the broker container periodically to cover process restarts as well as network cuts.
/// </summary>
/// <remarks>
/// Expect the broker log to fill with <c>disconnected: session taken over</c> for the publisher
/// client id — this is normal, not an error. Each chaos cut drops the socket ungracefully, so the
/// client reconnects with the same id before the broker has noticed the old TCP close; the broker
/// then evicts the stale connection (reason 0x8E, Session Taken Over) and hands the preserved
/// session to the new one. The eviction targets the already-dead old socket, so the client never
/// faults on it, and the resumed session (<c>SessionPresent = true</c>) keeps QoS 1 redelivery
/// intact. It is simply the broker-side signature of fast ungraceful reconnect.
/// </remarks>
[Trait("Category", "Soak")]
[Collection("mosquitto")]
public sealed class SoakTests
{
    private readonly MosquittoFixture _broker;

    public SoakTests(MosquittoFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task Sustained_load_through_endless_disconnects_leaks_nothing_and_loses_nothing()
    {
        var duration = ResolveDuration();
        using var lifetime = new CancellationTokenSource(duration + TimeSpan.FromSeconds(60));
        var topic = $"soak/{Guid.NewGuid():N}";

        await using var subscriber = new RawMqttClient(NewFactory());
        await subscriber.ConnectAsync(
            new MqttConnectPacket { ClientId = $"soak-sub-{Guid.NewGuid():N}", KeepAliveSeconds = 30 },
            lifetime.Token);
        await subscriber.SubscribeAsync(
            [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            lifetime.Token);

        // Verify zero loss with bounded memory: track the contiguous run of received sequence
        // numbers and a small set of out-of-order arrivals, rather than remembering every message
        // (which would itself grow O(messages) and mask the library's heap — the thing under test).
        var tracker = new ContiguousTracker();
        var collector = Task.Run(async () =>
        {
            await foreach (var message in subscriber.Messages.ReadAllAsync(lifetime.Token))
            {
                tracker.Observe(long.Parse(Encoding.UTF8.GetString(message.Payload.Span), CultureInfo.InvariantCulture));
            }
        }, lifetime.Token);

        var killable = new KillableTransportFactory(NewFactory());
        await using var publisher = new ResilientMqttClient(killable, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = $"soak-pub-{Guid.NewGuid():N}",
                KeepAliveSeconds = 30,
                CleanStart = false,
                SessionExpiryInterval = 600,
            },
            Backoff = new BackoffOptions { BaseDelay = TimeSpan.FromMilliseconds(50), MaxDelay = TimeSpan.FromSeconds(1) },
        });
        await publisher.StartAsync(lifetime.Token);
        await WaitConnectedAsync(publisher, lifetime.Token);

        // Warm up, then snapshot the heap once allocations have settled.
        await Task.Delay(TimeSpan.FromSeconds(2), lifetime.Token);
        var baseline = HeapBytes();

        using var chaos = new CancellationTokenSource();
        var random = new Random(20240613);
        var chaosLoop = Task.Run(async () =>
        {
            while (!chaos.IsCancellationRequested)
            {
                try { await Task.Delay(random.Next(150, 600), chaos.Token); }
                catch (OperationCanceledException) { return; }
                await killable.KillAsync();
            }
        }, lifetime.Token);

        long sequence = 0;
        long maxHeap = baseline;
        var deadline = DateTimeOffset.UtcNow + duration;
        while (DateTimeOffset.UtcNow < deadline)
        {
            await publisher.PublishAsync(
                new MqttPublishPacket
                {
                    Topic = topic,
                    Payload = Encoding.UTF8.GetBytes(sequence.ToString(CultureInfo.InvariantCulture)),
                    QualityOfService = MqttQualityOfService.AtLeastOnce,
                },
                lifetime.Token);
            sequence++;
            await Task.Delay(20, lifetime.Token);

            if (sequence % 200 == 0)
            {
                maxHeap = Math.Max(maxHeap, HeapBytes());
            }
        }

        var published = sequence;
        await chaos.CancelAsync();
        await chaosLoop;
        await WaitConnectedAsync(publisher, lifetime.Token);

        // Let the session resume and the queue fully drain.
        using var settle = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        while (tracker.Contiguous < published && !tracker.Overflowed && !settle.IsCancellationRequested)
        {
            await Task.Delay(200, lifetime.Token);
        }

        var finalHeap = HeapBytes();

        // Zero loss: every published sequence (0..published-1) arrived, forming one contiguous run.
        tracker.Overflowed.ShouldBeFalse("the contiguous run stalled too far behind — a message was lost");
        tracker.Contiguous.ShouldBe(published, $"received contiguous up to {tracker.Contiguous} of {published} ({tracker.OutOfOrder} still out of order), state={publisher.State}");
        publisher.State.ShouldBe(ConnectionState.Connected);

        // Bounded growth: the heap after a full drain is within a generous tolerance of the
        // post-warmup baseline (no unbounded accumulation of tasks/buffers/sessions). The tracker
        // above is O(reorder window), so this measures the library, not the test's bookkeeping.
        var growth = finalHeap - baseline;
        growth.ShouldBeLessThan(32 * 1024 * 1024, $"heap grew {growth / (1024 * 1024)} MB over {published} messages (peak {maxHeap / (1024 * 1024)} MB)");

        await publisher.StopAsync(lifetime.Token);
        await subscriber.DisconnectAsync(lifetime.Token);
    }

    private static long HeapBytes()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    private static TimeSpan ResolveDuration()
    {
        var configured = Environment.GetEnvironmentVariable("PULSE_SOAK_DURATION");
        return TimeSpan.TryParse(configured, CultureInfo.InvariantCulture, out var parsed) && parsed > TimeSpan.Zero
            ? parsed
            : TimeSpan.FromSeconds(30);
    }

    private TcpTransportFactory NewFactory() =>
        new(new TcpTransportOptions { Host = _broker.Host, Port = _broker.Port });

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

    /// <summary>
    /// Tracks how far an in-order sequence of integers has been received contiguously, holding only
    /// the out-of-order arrivals that have not yet joined the run — so memory is bounded by the
    /// reorder window, not the total message count. Duplicates (re-delivered messages) are ignored.
    /// </summary>
    private sealed class ContiguousTracker
    {
        // A real loss stalls the run forever, after which every later message piles up here; cap it
        // so a leak in the harness can never masquerade as a leak in the library under test.
        private const int OutOfOrderCap = 100_000;

        private readonly object _gate = new();
        private readonly HashSet<long> _ahead = [];
        private long _next;
        private bool _overflowed;

        public long Contiguous
        {
            get { lock (_gate) { return _next; } }
        }

        public int OutOfOrder
        {
            get { lock (_gate) { return _ahead.Count; } }
        }

        public bool Overflowed
        {
            get { lock (_gate) { return _overflowed; } }
        }

        public void Observe(long sequence)
        {
            lock (_gate)
            {
                if (sequence == _next)
                {
                    _next++;
                    while (_ahead.Remove(_next))
                    {
                        _next++;
                    }
                }
                else if (sequence > _next)
                {
                    if (_ahead.Count < OutOfOrderCap)
                    {
                        _ahead.Add(sequence);
                    }
                    else
                    {
                        _overflowed = true;
                    }
                }

                // sequence < _next: a duplicate from re-delivery — ignore.
            }
        }
    }
}
