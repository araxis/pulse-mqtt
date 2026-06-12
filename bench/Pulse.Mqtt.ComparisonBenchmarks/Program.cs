using System.Diagnostics;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using DotNet.Testcontainers.Builders;
using MQTTnet;
using MQTTnet.Protocol;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.ComparisonBenchmarks;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;

var container = new ContainerBuilder()
    .WithImage("eclipse-mosquitto:2")
    .WithCommand("mosquitto", "-c", "/mosquitto-no-auth.conf")
    .WithPortBinding(1883, assignRandomHostPort: true)
    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(1883))
    .Build();
await container.StartAsync();
Broker.Host = container.Hostname;
Broker.Port = container.GetMappedPublicPort(1883);
Console.WriteLine($"Mosquitto at {Broker.Host}:{Broker.Port}");

await ConnectLatencyPassAsync();
await ThroughputPassAsync();

// Round trips over the proxied loopback are noisy; ShortRun's three iterations swing run to
// run, so the latency comparison needs a wider sample.
var config = ManualConfig.Create(DefaultConfig.Instance)
    .AddJob(Job.Default
        .WithToolchain(InProcessEmitToolchain.Instance)
        .WithWarmupCount(5)
        .WithIterationCount(15))
    .AddDiagnoser(MemoryDiagnoser.Default)
    .WithOptions(ConfigOptions.DisableOptimizationsValidator);
BenchmarkRunner.Run<RoundTripBenchmarks>(config);

await container.DisposeAsync();
return;

static async Task ConnectLatencyPassAsync()
{
    Console.WriteLine();
    Console.WriteLine("== Connect latency (30 connect/disconnect cycles, median) ==");

    var pulse = new List<double>();
    for (var i = 0; i < 30; i++)
    {
        var sw = Stopwatch.StartNew();
        await using var client = new ResilientMqttClient(
            new TcpTransportFactory(new TcpTransportOptions { Host = Broker.Host, Port = Broker.Port }),
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = $"pulse-conn-{i}", KeepAliveSeconds = 30 },
            });
        await client.StartAsync(CancellationToken.None);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Yield();
        }

        pulse.Add(sw.Elapsed.TotalMilliseconds);
        await client.StopAsync(CancellationToken.None);
    }

    var mqttNet = new List<double>();
    var factory = new MqttClientFactory();
    for (var i = 0; i < 30; i++)
    {
        var sw = Stopwatch.StartNew();
        using var client = factory.CreateMqttClient();
        await client.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer(Broker.Host, Broker.Port)
            .WithClientId($"net-conn-{i}")
            .Build());
        mqttNet.Add(sw.Elapsed.TotalMilliseconds);
        await client.DisconnectAsync();
    }

    Console.WriteLine($"Pulse.Mqtt : {Median(pulse):F2} ms");
    Console.WriteLine($"MQTTnet    : {Median(mqttNet):F2} ms");
}

static async Task ThroughputPassAsync()
{
    const int total = 20_000;
    const int window = 200;
    var payload = new byte[64];
    Random.Shared.NextBytes(payload);

    Console.WriteLine();
    Console.WriteLine($"== Sustained throughput: {total:N0} QoS1 publishes, {window} in flight ==");

    // Pulse
    {
        await using var client = new ResilientMqttClient(
            new TcpTransportFactory(new TcpTransportOptions { Host = Broker.Host, Port = Broker.Port }),
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = "pulse-tp", KeepAliveSeconds = 30 },
            });
        await client.StartAsync(CancellationToken.None);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Yield();
        }

        var (elapsed, allocated, collections) = await MeasureAsync(async () =>
        {
            for (var sent = 0; sent < total; sent += window)
            {
                await Task.WhenAll(Enumerable.Range(0, window).Select(async _ =>
                {
                    var outcome = await client.PublishAsync(
                        new MqttPublishPacket
                        {
                            Topic = "bench/tp/pulse",
                            Payload = payload,
                            QualityOfService = MqttQualityOfService.AtLeastOnce,
                        },
                        CancellationToken.None);
                    if (outcome.Disposition != PublishDisposition.Delivered)
                    {
                        throw new InvalidOperationException($"Publish was {outcome.Disposition}, not delivered.");
                    }
                }));
            }
        });
        Report("Pulse.Mqtt", total, elapsed, allocated, collections);
    }

    // MQTTnet, with its default packet fragmentation and with single-write packets.
    foreach (var (label, fragmented) in new[] { ("MQTTnet", true), ("MQTTnet nf", false) })
    {
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(Broker.Host, Broker.Port)
            .WithClientId($"net-tp-{(fragmented ? "frag" : "single")}");
        if (!fragmented)
        {
            builder = builder.WithoutPacketFragmentation();
        }

        using var client = new MqttClientFactory().CreateMqttClient();
        await client.ConnectAsync(builder.Build());

        var (elapsed, allocated, collections) = await MeasureAsync(async () =>
        {
            for (var sent = 0; sent < total; sent += window)
            {
                await Task.WhenAll(Enumerable.Range(0, window).Select(_ =>
                    client.PublishAsync(new MqttApplicationMessageBuilder()
                        .WithTopic("bench/tp/net")
                        .WithPayload(payload)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build())));
            }
        });
        Report(label, total, elapsed, allocated, collections);
    }
}

static async Task<(TimeSpan Elapsed, long Allocated, (int Gen0, int Gen1, int Gen2) Collections)> MeasureAsync(Func<Task> work)
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    var gen0 = GC.CollectionCount(0);
    var gen1 = GC.CollectionCount(1);
    var gen2 = GC.CollectionCount(2);
    var allocated = GC.GetTotalAllocatedBytes(precise: true);
    var sw = Stopwatch.StartNew();

    await work();

    sw.Stop();
    return (
        sw.Elapsed,
        GC.GetTotalAllocatedBytes(precise: true) - allocated,
        (GC.CollectionCount(0) - gen0, GC.CollectionCount(1) - gen1, GC.CollectionCount(2) - gen2));
}

static void Report(string name, int total, TimeSpan elapsed, long allocated, (int Gen0, int Gen1, int Gen2) collections)
{
    Console.WriteLine(
        $"{name,-11}: {total / elapsed.TotalSeconds,10:N0} msg/s | {elapsed.TotalMilliseconds,8:N0} ms | " +
        $"{allocated / (double)total,8:N0} B/msg | GC {collections.Gen0}/{collections.Gen1}/{collections.Gen2}");
}

static double Median(List<double> values)
{
    var sorted = values.Order().ToArray();
    return sorted[sorted.Length / 2];
}

internal static class Broker
{
    public static string Host = "127.0.0.1";
    public static int Port = 1883;
}
