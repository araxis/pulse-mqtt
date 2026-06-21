using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Testing;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// The full client publish path: 10,000 QoS 0 messages through <see cref="ResilientMqttClient"/>
/// to an in-process broker, each publish awaited.
/// </summary>
[MemoryDiagnoser]
public class MessageProcessingBenchmarks
{
    private readonly MqttPublishPacket _message = new() { Topic = "A" };
    private PulseMqttTestBroker _broker = null!;
    private ResilientMqttClient _client = null!;

    [GlobalSetup]
    public void Setup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _broker = new PulseMqttTestBroker();
        _client = new ResilientMqttClient(_broker, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "bench", KeepAliveSeconds = 0 },
        });
        await _client.ConnectAsync(CancellationToken.None);
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
    public async Task Send_10000_Messages()
    {
        for (var i = 0; i < 10_000; i++)
        {
            var outcome = await _client.PublishAsync(_message, CancellationToken.None);
            if (outcome.Disposition != PublishDisposition.Delivered)
            {
                throw new InvalidOperationException($"Publish was {outcome.Disposition}, not delivered.");
            }
        }
    }
}
