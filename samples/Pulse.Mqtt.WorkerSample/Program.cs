using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pulse.Mqtt;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Testing;
using Pulse.Mqtt.WorkerSample;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Information);

var mqttHost = builder.Configuration["Mqtt:Host"];
var mqttPort = ReadInt(builder.Configuration["Mqtt:Port"], 1883);
var protocolVersion = ReadProtocolVersion(builder.Configuration["Mqtt:ProtocolVersion"], MqttProtocolVersion.V500);
var useInProcessBroker = string.IsNullOrWhiteSpace(mqttHost);

if (useInProcessBroker)
{
    builder.Services.AddSingleton<PulseMqttTestBroker>();
}

var mqtt = builder.Services
    .AddPulseMqttClient(WorkerMqttClients.Telemetry, options =>
    {
        options.Host = useInProcessBroker ? "in-process" : mqttHost!;
        options.Port = mqttPort;
        options.ClientId = $"worker-sample-{Environment.MachineName}";
        options.KeepAliveSeconds = 30;
        options.ProtocolVersion = protocolVersion;
    })
    .UseSerializer(_ => new JsonMqttSerializer(WorkerSampleJsonContext.Default));

if (useInProcessBroker)
{
    mqtt.UseTransportFactory(provider => provider.GetRequiredService<PulseMqttTestBroker>());
}

builder.Services.AddHostedService<TelemetryPipelineWorker>();

using var host = builder.Build();
await host.RunAsync();

static int ReadInt(string? value, int defaultValue) =>
    int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
        ? parsed
        : defaultValue;

static MqttProtocolVersion ReadProtocolVersion(string? value, MqttProtocolVersion defaultValue) =>
    Enum.TryParse<MqttProtocolVersion>(value, ignoreCase: true, out var parsed)
        ? parsed
        : defaultValue;
