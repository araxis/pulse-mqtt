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
/// Clients are process-wide singletons: BenchmarkDotNet runs setup once per benchmark case, and
/// reconnecting under a reused client id makes the broker revoke the previous session mid-run.
/// </summary>
public class RoundTripBenchmarks
{
    private static readonly byte[] Payload = CreatePayload();
    private static readonly SemaphoreSlim InitGate = new(1, 1);
    private static ResilientMqttClient? _pulse;
    private static IMqttClient? _mqttNet;
    private static volatile TaskCompletionSource? _pulseReceived;
    private static volatile TaskCompletionSource? _mqttNetReceived;

    [GlobalSetup]
    public async Task Setup()
    {
        await InitGate.WaitAsync();
        try
        {
            if (_pulse is not null)
            {
                return;
            }

            var pulse = new ResilientMqttClient(
                new TcpTransportFactory(new TcpTransportOptions { Host = Broker.Host, Port = Broker.Port }),
                new ResilientMqttClientOptions
                {
                    Connect = new MqttConnectPacket
                    {
                        ClientId = $"pulse-bench-{Guid.NewGuid():N}",
                        KeepAliveSeconds = 60,
                    },
                });
            await pulse.StartAsync(CancellationToken.None);
            while (pulse.State != ConnectionState.Connected)
            {
                await Task.Delay(1);
            }

            await pulse.SubscribeAsync([new MqttTopicFilter("bench/rt/pulse")], CancellationToken.None);
            _ = Task.Run(async () =>
            {
                await foreach (var _ in pulse.Messages.ReadAllAsync())
                {
                    _pulseReceived?.TrySetResult();
                }
            });

            var mqttNet = new MqttClientFactory().CreateMqttClient();
            mqttNet.ApplicationMessageReceivedAsync += _ =>
            {
                _mqttNetReceived?.TrySetResult();
                return Task.CompletedTask;
            };
            await mqttNet.ConnectAsync(new MqttClientOptionsBuilder()
                .WithTcpServer(Broker.Host, Broker.Port)
                .WithClientId($"net-bench-{Guid.NewGuid():N}")
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(60))
                .Build());
            await mqttNet.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(f => f.WithTopic("bench/rt/net").WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                .Build());

            _mqttNet = mqttNet;
            _pulse = pulse;
        }
        finally
        {
            InitGate.Release();
        }
    }

    [Benchmark]
    public async Task Pulse_Qos0_RoundTrip()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pulseReceived = received;
        await _pulse!.PublishAsync(
            new MqttPublishPacket { Topic = "bench/rt/pulse", Payload = Payload },
            CancellationToken.None);
        await received.Task;
    }

    [Benchmark]
    public async Task MqttNet_Qos0_RoundTrip()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _mqttNetReceived = received;
        await _mqttNet!.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("bench/rt/net")
            .WithPayload(Payload)
            .Build());
        await received.Task;
    }

    [Benchmark]
    public async Task Pulse_Qos1_PublishAck()
    {
        var outcome = await _pulse!.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "bench/sink",
                Payload = Payload,
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            CancellationToken.None);
        ThrowIfNotDelivered(outcome);
    }

    [Benchmark]
    public Task MqttNet_Qos1_PublishAck() =>
        _mqttNet!.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("bench/sink")
            .WithPayload(Payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build());

    [Benchmark]
    public async Task Pulse_Qos2_PublishAck()
    {
        var outcome = await _pulse!.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "bench/sink",
                Payload = Payload,
                QualityOfService = MqttQualityOfService.ExactlyOnce,
            },
            CancellationToken.None);
        ThrowIfNotDelivered(outcome);
    }

    [Benchmark]
    public Task MqttNet_Qos2_PublishAck() =>
        _mqttNet!.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("bench/sink")
            .WithPayload(Payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce)
            .Build());

    // A dead connection diverts publishes to the offline queue at memory speed; failing loudly
    // keeps the measurement an acknowledgement round trip instead of an enqueue.
    private static void ThrowIfNotDelivered(PublishOutcome outcome)
    {
        if (outcome.Disposition != PublishDisposition.Delivered)
        {
            throw new InvalidOperationException($"Publish was {outcome.Disposition}, not delivered.");
        }
    }

    private static byte[] CreatePayload()
    {
        var payload = new byte[64];
        Random.Shared.NextBytes(payload);
        return payload;
    }
}
