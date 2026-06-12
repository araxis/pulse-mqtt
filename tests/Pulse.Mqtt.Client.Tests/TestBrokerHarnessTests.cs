using System.Text;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>Consumer-style tests: only the public surface, no scripting, no external broker.</summary>
public sealed class TestBrokerHarnessTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private static ResilientMqttClientOptions NewOptions(string clientId) => new()
    {
        Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
    };

    [Fact]
    public async Task A_client_connects_with_zero_choreography()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("c1"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task An_own_publish_round_trips_through_a_route()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("c1"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await client.OnAsync("sensors/{id}", (message, values, _) =>
        {
            received.TrySetResult($"{values["id"]}:{Encoding.UTF8.GetString(message.Payload.Span)}");
            return ValueTask.CompletedTask;
        }, cancellationToken: timeout.Token);

        var outcome = await client.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "sensors/7",
                Payload = "21.5"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);

        outcome.Disposition.ShouldBe(PublishDisposition.Delivered);
        (await received.Task.WaitAsync(SafetyTimeout)).ShouldBe("7:21.5");
    }

    [Fact]
    public async Task Two_clients_exchange_messages()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var subscriber = new ResilientMqttClient(broker, NewOptions("sub"));
        await using var publisher = new ResilientMqttClient(broker, NewOptions("pub"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await subscriber.StartAsync(timeout.Token);
        await publisher.StartAsync(timeout.Token);
        await WaitForStateAsync(subscriber, ConnectionState.Connected, timeout.Token);
        await WaitForStateAsync(publisher, ConnectionState.Connected, timeout.Token);

        await using var stream = await subscriber.OpenStreamAsync("jobs/{kind}", cancellationToken: timeout.Token);

        await publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "jobs/created",
                Payload = "j-1"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);

        var routed = await stream.Reader.ReadAsync(timeout.Token);
        routed.Values["kind"].ShouldBe("created");
        Encoding.UTF8.GetString(routed.Message.Payload.Span).ShouldBe("j-1");
    }

    [Fact]
    public async Task Broker_injected_messages_reach_subscribers()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("c1"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await using var stream = await client.OpenStreamAsync("alerts/#", cancellationToken: timeout.Token);
        await broker.PublishAsync(new MqttPublishPacket { Topic = "alerts/temp", Payload = "hot"u8.ToArray() }, timeout.Token);

        var routed = await stream.Reader.ReadAsync(timeout.Token);
        Encoding.UTF8.GetString(routed.Message.Payload.Span).ShouldBe("hot");
    }

    [Fact]
    public async Task The_broker_records_client_publishes()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("c1"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await client.PublishAsync(new MqttPublishPacket { Topic = "audit/x", Payload = "p"u8.ToArray() }, timeout.Token);

        var recorded = await broker.ClientPublishes.ReadAsync(timeout.Token);
        recorded.Topic.ShouldBe("audit/x");
        Encoding.UTF8.GetString(recorded.Payload.Span).ShouldBe("p");
    }

    [Fact]
    public async Task A_qos2_publish_completes_as_delivered()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("c1"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var outcome = await client.PublishAsync(
            new MqttPublishPacket { Topic = "exact/1", QualityOfService = MqttQualityOfService.ExactlyOnce },
            timeout.Token);

        outcome.Disposition.ShouldBe(PublishDisposition.Delivered);
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken ct)
    {
        while (client.State != state)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Delay(5, ct);
        }
    }
}
