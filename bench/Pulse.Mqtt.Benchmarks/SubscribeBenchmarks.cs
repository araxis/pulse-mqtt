using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Testing;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Subscription churn: 10,000 single-topic SUBSCRIBE round trips against an in-process broker,
/// each acknowledgement awaited.
/// </summary>
[MemoryDiagnoser]
public class SubscribeBenchmarks
{
    private PulseMqttTestBroker _broker = null!;
    private ResilientMqttClient _client = null!;
    private List<string> _topics = [];

    [GlobalSetup]
    public void Setup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _topics = TopicGenerator.GenerateTopicsByPublisher(1, 10_000).Values.First();

        _broker = new PulseMqttTestBroker();
        _client = new ResilientMqttClient(_broker, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "bench", KeepAliveSeconds = 0 },
        });
        await _client.StartAsync(CancellationToken.None);
        while (_client.State != ConnectionState.Connected)
        {
            await Task.Yield();
        }
    }

    [GlobalCleanup]
    public void Cleanup() => CleanupAsync().GetAwaiter().GetResult();

    private async Task CleanupAsync()
    {
        await _client.DisposeAsync();
        await _broker.DisposeAsync();
    }

    [Benchmark]
    public async Task Subscribe_10000_Topics()
    {
        foreach (var topic in _topics)
        {
            await _client.SubscribeAsync([new MqttTopicFilter(topic)], CancellationToken.None);
        }
    }
}
