using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Pulse.Mqtt;
using Pulse.Mqtt.AspNetCoreSample;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Testing;

var builder = WebApplication.CreateBuilder(args);

var mqttHost = builder.Configuration["Mqtt:Host"];
var mqttPort = ReadInt(builder.Configuration["Mqtt:Port"], 1883);
var protocolVersion = ReadProtocolVersion(builder.Configuration["Mqtt:ProtocolVersion"], MqttProtocolVersion.V500);
var useInProcessBroker = string.IsNullOrWhiteSpace(mqttHost);

if (useInProcessBroker)
{
    builder.Services.AddSingleton<PulseMqttTestBroker>();
}

builder.Services.AddSingleton<TelemetryStore>();
builder.Services.AddHostedService<TelemetryRouteService>();

var mqtt = builder.Services
    .AddPulseMqttClient(SampleMqttClients.Telemetry, options =>
    {
        options.Host = useInProcessBroker ? "in-process" : mqttHost!;
        options.Port = mqttPort;
        options.ClientId = $"aspnet-sample-{Environment.MachineName}";
        options.KeepAliveSeconds = 30;
        options.ProtocolVersion = protocolVersion;
    })
    .UseSerializer(_ => new JsonMqttSerializer(AspNetSampleJsonContext.Default))
    .AddHealthCheck(options =>
    {
        options.DegradedOfflineQueueDepthThreshold = 100;
        options.UnhealthyOfflineQueueDepthThreshold = 1_000;
        options.DegradedPendingSubscriptionOperationsThreshold = 10;
        options.UnhealthyPendingSubscriptionOperationsThreshold = 100;
    });

if (useInProcessBroker)
{
    mqtt.UseTransportFactory(provider => provider.GetRequiredService<PulseMqttTestBroker>());
}

var app = builder.Build();

app.MapHealthChecks("/health");
app.MapHealthChecks("/ready", new HealthCheckOptions
{
    Predicate = registration => registration.Name == $"pulse-mqtt-{SampleMqttClients.Telemetry}",
});
app.MapHealthChecks("/live", new HealthCheckOptions
{
    Predicate = registration => registration.Name == $"pulse-mqtt-{SampleMqttClients.Telemetry}",
    ResultStatusCodes =
    {
        [HealthStatus.Healthy] = StatusCodes.Status200OK,
        [HealthStatus.Degraded] = StatusCodes.Status200OK,
        [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
    },
});

var api = app.MapGroup("/api");

api.MapPost(
    "/devices/{deviceId}/telemetry",
    async (
        string deviceId,
        TelemetryPublishRequest request,
        [FromKeyedServices(SampleMqttClients.Telemetry)] ResilientMqttClient client,
        CancellationToken cancellationToken) =>
    {
        var reading = new TelemetryReading(
            request.Unit,
            request.Value,
            request.Timestamp ?? DateTimeOffset.UtcNow);

        var topic = $"sensors/{deviceId}/telemetry";
        var outcome = await client.PublishAsync(
            topic,
            reading,
            MqttQualityOfService.AtLeastOnce,
            cancellationToken: cancellationToken);

        return TypedResults.Accepted(
            $"/api/devices/{deviceId}/telemetry",
            new TelemetryPublishResponse(deviceId, topic, outcome.Disposition.ToString(), outcome.ReasonCode.ToString()));
    });

api.MapGet(
    "/devices/{deviceId}/telemetry/latest",
    (string deviceId, TelemetryStore store) =>
        store.TryGet(deviceId, out var reading)
            ? Results.Ok(reading)
            : Results.NotFound(new { deviceId, message = "No telemetry has been routed for this device." }));

api.MapGet(
    "/mqtt/diagnostics",
    ([FromKeyedServices(SampleMqttClients.Telemetry)] ResilientMqttClient client) =>
    {
        var snapshot = client.GetDiagnosticsSnapshot();
        return TypedResults.Ok(MqttDiagnosticsResponse.From(snapshot));
    });

api.MapGet(
    "/mqtt/capabilities",
    ([FromKeyedServices(SampleMqttClients.Telemetry)] ResilientMqttClient client) =>
    {
        var capabilities = client.GetBrokerCapabilitiesSnapshot();
        return TypedResults.Ok(MqttCapabilitiesResponse.From(capabilities));
    });

api.MapGet(
    "/",
    () => new
    {
        publish = "POST /api/devices/boiler-1/telemetry",
        latest = "GET /api/devices/boiler-1/telemetry/latest",
        diagnostics = "GET /api/mqtt/diagnostics",
        capabilities = "GET /api/mqtt/capabilities",
        health = "GET /health, /ready, /live",
    });

app.Run();

static int ReadInt(string? value, int defaultValue) =>
    int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
        ? parsed
        : defaultValue;

static MqttProtocolVersion ReadProtocolVersion(string? value, MqttProtocolVersion defaultValue) =>
    Enum.TryParse<MqttProtocolVersion>(value, ignoreCase: true, out var parsed)
        ? parsed
        : defaultValue;
