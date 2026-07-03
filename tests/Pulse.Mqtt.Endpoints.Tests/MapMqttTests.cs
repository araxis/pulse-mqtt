using System.Text;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Endpoints;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Serialization.Json;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Endpoints.Tests;

public sealed record Reading(string Sensor, double Value);

[JsonSerializable(typeof(Reading))]
public sealed partial class EndpointsJsonContext : JsonSerializerContext;

public sealed class MapMqttTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Map_subscribes_and_dispatches_with_typed_route_values()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var received = new TaskCompletionSource<(int Device, string Text)>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt("sensors/{deviceId:int}/temp", context =>
        {
            received.TrySetResult((context.Route.GetInt("deviceId"), Encoding.UTF8.GetString(context.Message.Payload.Span)));
            return ValueTask.CompletedTask;
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "sensors/42/temp", Payload = "21.5"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);

        var (device, text) = await received.Task.WaitAsync(timeout.Token);
        device.ShouldBe(42);
        text.ShouldBe("21.5");
    }

    [Fact]
    public async Task A_constrained_endpoint_never_sees_non_conforming_topics()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var conforming = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var invocations = 0;
        await using var endpoint = client.MapMqtt("sensors/{deviceId:int}/temp", context =>
        {
            Interlocked.Increment(ref invocations);
            conforming.TrySetResult(context.Route.GetInt("deviceId"));
            return ValueTask.CompletedTask;
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        // The broker delivers both (the filter is sensors/+/temp), but the constraint keeps the
        // non-conforming one away from the handler.
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "sensors/oven/temp", Payload = "hot"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "sensors/7/temp", Payload = "ok"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);

        (await conforming.Task.WaitAsync(timeout.Token)).ShouldBe(7);
        invocations.ShouldBe(1);
    }

    [Fact]
    public async Task A_typed_endpoint_deserializes_the_payload()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker, withSerializer: true);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var received = new TaskCompletionSource<(Reading Reading, int Device)>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt<Reading>("sensors/{deviceId:int}/reading", (reading, context) =>
        {
            received.TrySetResult((reading, context.Route.GetInt("deviceId")));
            return ValueTask.CompletedTask;
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker, withSerializer: true);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync("sensors/9/reading", new Reading("dht22", 21.5), cancellationToken: timeout.Token);

        var (reading, device) = await received.Task.WaitAsync(timeout.Token);
        reading.ShouldBe(new Reading("dht22", 21.5));
        device.ShouldBe(9);
    }

    [Fact]
    public async Task Each_invocation_gets_its_own_service_scope()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var services = new ServiceCollection().AddScoped<ScopeWitness>().BuildServiceProvider();
        var witnesses = new List<ScopeWitness>();
        var seen = new SemaphoreSlim(0);
        await using var endpoint = client.MapMqtt(
            "jobs/{id}",
            context =>
            {
                lock (witnesses)
                {
                    witnesses.Add(context.Services.GetRequiredService<ScopeWitness>());
                }

                seen.Release();
                return ValueTask.CompletedTask;
            },
            services: services);
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "jobs/a", Payload = "1"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "jobs/b", Payload = "2"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);

        await seen.WaitAsync(timeout.Token);
        await seen.WaitAsync(timeout.Token);

        witnesses.Count.ShouldBe(2);
        witnesses[0].ShouldNotBeSameAs(witnesses[1]); // a fresh scope per message
        WaitForDisposal(witnesses, timeout.Token);
    }

    [Fact]
    public async Task Disposing_the_endpoint_stops_delivery()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var invocations = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var endpoint = client.MapMqtt("alerts/{level}", _ =>
        {
            Interlocked.Increment(ref invocations);
            first.TrySetResult();
            return ValueTask.CompletedTask;
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/red", Payload = "x"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await first.Task.WaitAsync(timeout.Token);

        await endpoint.DisposeAsync();

        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/red", Payload = "y"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await Task.Delay(200, timeout.Token); // deliveries, if any, would land well within this

        invocations.ShouldBe(1);
    }

    [Fact]
    public async Task Subscribed_faults_when_the_broker_denies_the_filter()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            SubAckFactory = context => context.DefaultSubAck with
            {
                ReasonCodes = [.. context.Subscribe.TopicFilters.Select(_ => MqttReasonCode.NotAuthorized)],
            },
        });
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var endpoint = client.MapMqtt("secret/{area}", _ => ValueTask.CompletedTask);
        var denied = await Should.ThrowAsync<MqttException>(() => endpoint.Subscribed.WaitAsync(timeout.Token));
        denied.Message.ShouldContain("NotAuthorized");
    }

    private static void WaitForDisposal(List<ScopeWitness> witnesses, CancellationToken token)
    {
        while (true)
        {
            lock (witnesses)
            {
                if (witnesses.All(witness => witness.Disposed))
                {
                    return;
                }
            }

            token.ThrowIfCancellationRequested();
            Thread.Sleep(10);
        }
    }

    private static ResilientMqttClient NewClient(PulseMqttTestBroker broker, bool withSerializer = false) =>
        new(broker, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = $"ep-{Guid.NewGuid():N}"[..16], KeepAliveSeconds = 0 },
            Serializer = withSerializer ? new JsonMqttSerializer(EndpointsJsonContext.Default) : null,
        });

    private sealed class ScopeWitness : IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }
}
