using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Testing;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Fan-out delivery through the broker's subscription matching: ten subscriber clients spread
/// over a large topic space, one message published per subscribed topic, the operation completing
/// when every delivery arrived.
/// </summary>
[MemoryDiagnoser]
public class MessageDeliveryBenchmarks
{
    private const int PublisherCount = 1000;
    private const int SubscriberCount = 10;

    private readonly byte[] _payload = [1, 2, 3, 4];
    private PulseMqttTestBroker _broker = null!;
    private ResilientMqttClient _publisher = null!;
    private readonly List<ResilientMqttClient> _subscribers = [];
    private List<string> _subscribedTopics = [];
    private int _expected;
    private int _received;
    private volatile TaskCompletionSource? _allDelivered;

    [Params(1, 5)]
    public int TopicsPerPublisher { get; set; }

    [Params(5, 10, 20, 50)]
    public int SubscribedTopicsPerSubscriber { get; set; }

    [GlobalSetup]
    public void Setup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        var topicsByPublisher = TopicGenerator.GenerateTopicsByPublisher(PublisherCount, TopicsPerPublisher);
        var allTopics = topicsByPublisher.Values.SelectMany(topics => topics).ToList();

        _broker = new PulseMqttTestBroker();
        _publisher = await ConnectAsync("publisher");

        var totalSubscribed = SubscribedTopicsPerSubscriber * SubscriberCount;
        var step = allTopics.Count / totalSubscribed;
        if (step * totalSubscribed != allTopics.Count)
        {
            throw new InvalidOperationException(
                $"{allTopics.Count} topics do not spread evenly over {totalSubscribed} subscriptions.");
        }

        _subscribedTopics = new List<string>(totalSubscribed);
        var topicIndex = 0;
        for (var i = 0; i < SubscriberCount; i++)
        {
            var subscriber = await ConnectAsync("subscriber" + i);
            _subscribers.Add(subscriber);

            for (var s = 0; s < SubscribedTopicsPerSubscriber; s++, topicIndex += step)
            {
                var topic = allTopics[topicIndex];
                _subscribedTopics.Add(topic);
                await subscriber.SubscribeAsync([new MqttTopicFilter(topic)], CancellationToken.None);
            }

            _ = Task.Run(async () =>
            {
                await foreach (var _ in subscriber.Messages.ReadAllAsync())
                {
                    if (Interlocked.Increment(ref _received) == _expected)
                    {
                        _allDelivered?.TrySetResult();
                    }
                }
            });
        }
    }

    [GlobalCleanup]
    public void Cleanup() => CleanupAsync().GetAwaiter().GetResult();

    private async Task CleanupAsync()
    {
        await _publisher.DisposeAsync();
        foreach (var subscriber in _subscribers)
        {
            await subscriber.DisposeAsync();
        }

        await _broker.DisposeAsync();
    }

    [Benchmark]
    public async Task DeliverMessages()
    {
        _expected = _subscribedTopics.Count;
        Interlocked.Exchange(ref _received, 0);
        var allDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _allDelivered = allDelivered;

        foreach (var topic in _subscribedTopics)
        {
            await _publisher.PublishAsync(
                new MqttPublishPacket { Topic = topic, Payload = _payload },
                CancellationToken.None);
        }

        await allDelivered.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }

    private async Task<ResilientMqttClient> ConnectAsync(string clientId)
    {
        var client = new ResilientMqttClient(_broker, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
        });
        await client.ConnectAsync(CancellationToken.None);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Yield();
        }

        return client;
    }
}
