using System.Text.Json.Serialization;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Serialization.Json;
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
await client.StartAsync(timeout.Token);
while (client.State != ConnectionState.Connected)
{
    await Task.Delay(10, timeout.Token);
}

var received = new TaskCompletionSource<SmokeReading>(TaskCreationOptions.RunContinuationsAsynchronously);
await client.OnAsync<SmokeReading>("smoke/{id}", (value, _, _) =>
{
    received.TrySetResult(value);
    return ValueTask.CompletedTask;
}, cancellationToken: timeout.Token);

var outcome = await client.PublishAsync("smoke/1", new SmokeReading("aot", 1.0), MqttQualityOfService.AtLeastOnce, cancellationToken: timeout.Token);
var reading = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));

await client.StopAsync(timeout.Token);
Console.WriteLine($"Smoke passed: disposition={outcome.Disposition}, value={reading.Value}, state={client.State}");

internal sealed record SmokeReading(string Source, double Value);

[JsonSerializable(typeof(SmokeReading))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
