using System.Text.Json.Serialization;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Endpoints;
using Pulse.Mqtt.Testing;

// Exercises the full public stack — broker, resilient client, routing, typed messaging — so
// trimming and AOT analysis cover the real code paths, and the published binary proves them.
await using var broker = new PulseMqttTestBroker();
await using var client = new ResilientMqttClient(broker, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "aot-smoke", KeepAliveSeconds = 0 },
    Serializer = new JsonMqttSerializer(SmokeJsonContext.Default),
});

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
await client.ConnectAsync(timeout.Token);
while (client.State != ConnectionState.Connected)
{
    await Task.Delay(10, timeout.Token);
}

var received = new TaskCompletionSource<SmokeReading>(TaskCreationOptions.RunContinuationsAsynchronously);
var template = MqttRouteTemplate.Parse("smoke/{id}");
await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], timeout.Token);
using var route = client.RegisterRoute<SmokeReading>(template, (value, _, _) =>
{
    received.TrySetResult(value);
    return ValueTask.CompletedTask;
});

var outcome = await client.PublishAsync("smoke/1", new SmokeReading("aot", 1.0), MqttQualityOfService.AtLeastOnce, cancellationToken: timeout.Token);
var reading = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));

// The endpoints layer must survive full AOT too: constrained template, typed route access.
var mapped = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
await using (var endpoint = client.MapMqtt("smoke/{id:int}/mapped", context =>
{
    mapped.TrySetResult(context.Route.GetInt("id"));
    return ValueTask.CompletedTask;
}))
{
    await endpoint.Subscribed.WaitAsync(timeout.Token);
    await client.PublishAsync(
        new Pulse.Mqtt.Packets.MqttPublishPacket
        {
            Topic = "smoke/7/mapped",
            Payload = "x"u8.ToArray(),
            QualityOfService = MqttQualityOfService.AtLeastOnce,
        },
        timeout.Token);
    if (await mapped.Task.WaitAsync(TimeSpan.FromSeconds(10)) != 7)
    {
        throw new InvalidOperationException("MapMqtt did not deliver the constrained route value.");
    }
}

// The generated Minimal-API-style shape must survive AOT as well: the interceptor lowers this
// lambda at compile time, so there is nothing reflective left to trim away.
var generated = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
await using (var generatedEndpoint = client.MapMqtt("smoke/{n:int}/generated", (int n, CancellationToken _) =>
{
    generated.TrySetResult(n);
    return ValueTask.CompletedTask;
}))
{
    await generatedEndpoint.Subscribed.WaitAsync(timeout.Token);
    await client.PublishAsync(
        new Pulse.Mqtt.Packets.MqttPublishPacket
        {
            Topic = "smoke/9/generated",
            Payload = "x"u8.ToArray(),
            QualityOfService = MqttQualityOfService.AtLeastOnce,
        },
        timeout.Token);
    if (await generated.Task.WaitAsync(TimeSpan.FromSeconds(10)) != 9)
    {
        throw new InvalidOperationException("The generated MapMqtt endpoint did not deliver.");
    }
}

await client.DisconnectAsync(timeout.Token);
Console.WriteLine($"Smoke passed: disposition={outcome.Disposition}, value={reading.Value}, state={client.State}");

internal sealed record SmokeReading(string Source, double Value);

[JsonSerializable(typeof(SmokeReading))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
