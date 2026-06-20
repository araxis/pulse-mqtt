using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Sample;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Testing;

// Runs against a real broker when one is given (for example: --host localhost --port 1883),
// and against the in-process test broker otherwise, so it works with no setup at all.
var brokerHost = ArgValue(args, "--host");
var brokerPort = int.TryParse(ArgValue(args, "--port"), out var port) ? port : 1883;
var inProcessBroker = brokerHost is null ? new PulseMqttTestBroker() : null;

Console.WriteLine(inProcessBroker is null
    ? $"Connecting to {brokerHost}:{brokerPort}."
    : "No --host given; using the in-process test broker.");

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);

var mqtt = builder.Services
    .AddPulseMqttClient("devices", options =>
    {
        options.Host = brokerHost ?? "in-process";
        options.Port = brokerPort;
        options.ClientId = $"sample-{Environment.MachineName}";
        options.KeepAliveSeconds = 30;
    })
    .UseSerializer(_ => new JsonMqttSerializer(SampleJsonContext.Default));

if (inProcessBroker is not null)
{
    mqtt.UseTransportFactory(_ => inProcessBroker);
}

using var host = builder.Build();
await host.StartAsync();

var client = host.Services.GetRequiredService<IPulseMqttClientFactory>().GetClient("devices");

// Watch connection state in the background; the supervisor reconnects on its own.
_ = Task.Run(async () =>
{
    await foreach (var change in client.WatchState())
    {
        Console.WriteLine($"[state] {change.Previous} -> {change.Current}");
    }
});

// 1. Routed subscription: {deviceId} is captured from the topic of every matching message.
await using var telemetryRoute = await client.OnAsync<TelemetryReading>(
    "sensors/{deviceId}/telemetry",
    MqttQualityOfService.AtLeastOnce,
    (reading, message, _) =>
    {
        Console.WriteLine(
            $"[telemetry] device={message.Values["deviceId"]} {reading.Value:F1}{reading.Unit} at {reading.Timestamp:HH:mm:ss}");
        return ValueTask.CompletedTask;
    });

// 2. Request/response: this client also acts as the responder for device status requests.
var started = DateTimeOffset.UtcNow;
var statusTemplate = MqttRouteTemplate.Parse("devices/{deviceId}/status");
await client.SubscribeAsync([statusTemplate.ToTopicFilter(MqttQualityOfService.AtLeastOnce)]);
using var statusResponder = client.RegisterRequestHandler<StatusRequest, StatusReply>(
    statusTemplate,
    (request, message, _) =>
    {
        var deviceId = message.Values["deviceId"];
        Console.WriteLine($"[responder] {request.Caller} asked for the status of {deviceId}");
        return ValueTask.FromResult(new StatusReply(deviceId, "online", DateTimeOffset.UtcNow - started));
    });

// 3. Typed publishes at QoS 1: each one is awaited to its broker acknowledgement.
foreach (var deviceId in new[] { "boiler-1", "boiler-2", "pump-7" })
{
    var reading = new TelemetryReading("C", 20 + Random.Shared.NextDouble() * 5, DateTimeOffset.UtcNow);
    var outcome = await client.PublishAsync(
        $"sensors/{deviceId}/telemetry",
        reading,
        MqttQualityOfService.AtLeastOnce);
    Console.WriteLine($"[publish] {deviceId}: {outcome.Disposition}");
}

// 4. A request to one device, answered by the responder above over response-topic correlation.
var reply = await client.RequestAsync<StatusRequest, StatusReply>(
    "devices/boiler-1/status",
    new StatusRequest(Environment.UserName));
Console.WriteLine($"[request] {reply.DeviceId} is {reply.State}, up {reply.Uptime.TotalSeconds:F0}s");

// Give routed deliveries a moment to drain before shutting down.
await Task.Delay(TimeSpan.FromMilliseconds(500));

await host.StopAsync();
if (inProcessBroker is not null)
{
    await inProcessBroker.DisposeAsync();
}

Console.WriteLine("Done.");
return;

static string? ArgValue(string[] args, string name)
{
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
}
