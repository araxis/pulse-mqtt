using BenchmarkDotNet.Attributes;
using MQTTnet;
using MQTTnet.Protocol;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.ComparisonBenchmarks;

/// <summary>
/// Per-operation comparison over one Mosquitto broker: a QoS 0 self-subscribed round trip and
/// QoS 1/2 publish acknowledgements, both libraries using their user-facing clients.
/// </summary>
public class RoundTripBenchmarks
{
    private readonly byte[] _payload = new byte[64];

    private ResilientMqttClient _pulse = null!;
    private IMqttClient _mqttNet = null!;
    private volatile TaskCompletionSource? _pulseReceived;
    private volatile TaskCompletionSource? _mqttNetReceived;

    [GlobalSetup]
    public async Task Setup()
    {
        Random.Shared.NextBytes(_payload);

        _pulse = new ResilientMqttClient(
            new TcpTransportFactory(new TcpTransportOptions { Host = Broker.Host, Port = Broker.Port }),
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = "pulse-bench", KeepAliveSeconds = 60 },
            });
        await _pulse.StartAsync(CancellationToken.None);
        while (_pulse.State != ConnectionState.Connected)
        {
            await Task.Delay(1);
        }

        await _pulse.SubscribeAsync([new MqttTopicFilter("bench/rt/pulse")], CancellationToken.None);
        _ = Task.Run(async () =>
        {
            await foreach (var _ in _pulse.Messages.ReadAllAsync())
            {
                _pulseReceived?.TrySetResult();
            }
        });

        _mqttNet = new MqttClientFactory().CreateMqttClient();
        _mqttNet.ApplicationMessageReceivedAsync += _ =>
        {
            _mqttNetReceived?.TrySetResult();
            return Task.CompletedTask;
        };
        await _mqttNet.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer(Broker.Host, Broker.Port)
            .WithClientId("net-bench")
            .Build());
        await _mqttNet.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("bench/rt/net").WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
            .Build());
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _pulse.DisposeAsync();
        await _mqttNet.DisconnectAsync();
        _mqttNet.Dispose();
    }

    [Benchmark]
    public async Task Pulse_Qos0_RoundTrip()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pulseReceived = received;
        await _pulse.PublishAsync(
            new MqttPublishPacket { Topic = "bench/rt/pulse", Payload = _payload },
            CancellationToken.None);
        await received.Task;
    }

    [Benchmark]
    public async Task MqttNet_Qos0_RoundTrip()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _mqttNetReceived = received;
        await _mqttNet.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("bench/rt/net")
            .WithPayload(_payload)
            .Build());
        await received.Task;
    }

    [Benchmark]
    public Task Pulse_Qos1_PublishAck() =>
        _pulse.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "bench/sink",
                Payload = _payload,
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            CancellationToken.None);

    [Benchmark]
    public Task MqttNet_Qos1_PublishAck() =>
        _mqttNet.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("bench/sink")
            .WithPayload(_payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build());

    [Benchmark]
    public Task Pulse_Qos2_PublishAck() =>
        _pulse.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "bench/sink",
                Payload = _payload,
                QualityOfService = MqttQualityOfService.ExactlyOnce,
            },
            CancellationToken.None);

    [Benchmark]
    public Task MqttNet_Qos2_PublishAck() =>
        _mqttNet.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("bench/sink")
            .WithPayload(_payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce)
            .Build());
}
