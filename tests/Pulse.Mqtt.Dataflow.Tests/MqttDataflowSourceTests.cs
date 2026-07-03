using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks.Dataflow;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Dataflow;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Dataflow.Tests;

public sealed class MqttDataflowSourceTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Message_source_block_forwards_inbound_publish_packets()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("messages"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        await client.SubscribeAsync([new MqttTopicFilter("raw/in")], timeout.Token);

        await using var source = client.ToMessageSourceBlock(cancellationToken: timeout.Token);

        await broker.PublishAsync(
            new MqttPublishPacket { Topic = "raw/in", Payload = "payload"u8.ToArray() },
            timeout.Token);

        var received = await source.ReceiveAsync(timeout.Token);
        received.Topic.ShouldBe("raw/in");
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("payload");
    }

    [Fact]
    public async Task Route_source_block_emits_matching_routed_messages()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("routes"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var template = MqttRouteTemplate.Parse("jobs/{kind}");
        await client.SubscribeAsync(template, MqttQualityOfService.AtMostOnce, timeout.Token);
        await using var source = client.ToRouteSourceBlock(template, cancellationToken: timeout.Token);

        await broker.PublishAsync(
            new MqttPublishPacket { Topic = "jobs/created", Payload = "j-1"u8.ToArray() },
            timeout.Token);

        var routed = await source.ReceiveAsync(timeout.Token);
        routed.Values["kind"].ShouldBe("created");
        Encoding.UTF8.GetString(routed.Message.Payload.Span).ShouldBe("j-1");
    }

    [Fact]
    public async Task Acknowledged_route_source_block_emits_acknowledged_messages()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("ack-routes"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        var template = MqttRouteTemplate.Parse("jobs/{id}");
        await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, timeout.Token);
        await using var source = client.ToAcknowledgedRouteSourceBlock(template, cancellationToken: timeout.Token);

        await broker.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "jobs/42",
                Payload = "run"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);

        var routed = await source.ReceiveAsync(timeout.Token);
        routed.Values["id"].ShouldBe("42");
        routed.IsAcknowledged.ShouldBeFalse();

        await routed.AcknowledgeAsync(timeout.Token);
        routed.IsAcknowledged.ShouldBeTrue();
    }

    [Fact]
    public async Task State_source_block_emits_connection_state_changes()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("states"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await using var source = client.ToStateSourceBlock(cancellationToken: timeout.Token);
        await Task.Yield();
        await client.DisconnectAsync(timeout.Token);

        var states = new List<ConnectionState>();
        while (!states.Contains(ConnectionState.Stopped))
        {
            states.Add((await source.ReceiveAsync(timeout.Token)).Current);
        }

        states.ShouldContain(ConnectionState.Stopped);
    }

    [Fact]
    public async Task Bounded_capacity_applies_backpressure()
    {
        using var release = new CancellationTokenSource(SafetyTimeout);
        var yieldedFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var yieldedSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var source = new MqttDataflowSource<int>(
            SlowSequence(yieldedFirst, yieldedSecond, release.Token),
            new MqttDataflowSourceOptions { BoundedCapacity = 1 },
            cancellationToken: release.Token);

        var receivable = (IReceivableSourceBlock<int>)source;
        await yieldedFirst.Task.WaitAsync(release.Token);

        await Should.ThrowAsync<TimeoutException>(
            yieldedSecond.Task.WaitAsync(TimeSpan.FromMilliseconds(100), release.Token));

        receivable.TryReceive(null, out var first).ShouldBeTrue();
        first.ShouldBe(1);
        await EventuallyAsync(() => receivable.TryReceive(null, out var value) && value == 2, release.Token);
        await yieldedSecond.Task.WaitAsync(SafetyTimeout, release.Token);
    }

    [Fact]
    public async Task Complete_stops_the_pump_and_releases_owned_route_resources()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("complete-route"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        await client.SubscribeAsync(MqttRouteTemplate.Parse("complete/{id}"), timeout.Token);

        await using var source = client.ToRouteSourceBlock("complete/{id}", cancellationToken: timeout.Token);
        source.Complete();
        await source.Completion.WaitAsync(timeout.Token);

        await broker.PublishAsync(new MqttPublishPacket { Topic = "complete/1" }, timeout.Token);
        ((IReceivableSourceBlock<MqttRoutedMessage>)source).TryReceive(null, out _).ShouldBeFalse();
    }

    [Fact]
    public async Task Dispose_stops_the_pump_and_releases_owned_route_resources()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("dispose-route"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        await client.SubscribeAsync(MqttRouteTemplate.Parse("dispose/{id}"), timeout.Token);

        var source = client.ToRouteSourceBlock("dispose/{id}", cancellationToken: timeout.Token);
        await source.DisposeAsync();

        await broker.PublishAsync(new MqttPublishPacket { Topic = "dispose/1" }, timeout.Token);
        ((IReceivableSourceBlock<MqttRoutedMessage>)source).TryReceive(null, out _).ShouldBeFalse();
    }

    [Fact]
    public async Task Route_source_block_with_invalid_options_does_not_leak_a_route()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("invalid-route-opts"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        await client.SubscribeAsync(MqttRouteTemplate.Parse("leak/{id}"), timeout.Token);

        // An invalid bounded capacity must throw AND leave no route registered. Before the fix the
        // route was opened before the throwing MqttDataflowSource constructor, so it leaked — capturing
        // matching inbound messages into an undrained channel instead of letting them fall through.
        Should.Throw<ArgumentOutOfRangeException>(() =>
            client.ToRouteSourceBlock("leak/{id}", sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 0 }));

        await broker.PublishAsync(new MqttPublishPacket { Topic = "leak/1" }, timeout.Token);
        (await client.Router.Unmatched.ReadAsync(timeout.Token)).Topic.ShouldBe("leak/1");
    }

    [Fact]
    public async Task Acknowledged_route_source_block_with_invalid_options_does_not_leak_a_route()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("invalid-ack-route-opts"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
        await client.SubscribeAsync(MqttRouteTemplate.Parse("leak-ack/{id}"), timeout.Token);

        Should.Throw<ArgumentOutOfRangeException>(() =>
            client.ToAcknowledgedRouteSourceBlock("leak-ack/{id}", sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 0 }));

        // A leaked acknowledged route would consume the message on the sink; with none, it falls
        // through to the regular router and surfaces as unmatched.
        await broker.PublishAsync(new MqttPublishPacket { Topic = "leak-ack/1" }, timeout.Token);
        (await client.Router.Unmatched.ReadAsync(timeout.Token)).Topic.ShouldBe("leak-ack/1");
    }

    [Fact]
    public async Task Source_stream_failures_fault_the_block()
    {
        await using var source = new MqttDataflowSource<int>(FaultingSequence());

        var error = await Should.ThrowAsync<InvalidOperationException>(source.Completion);
        error.Message.ShouldBe("source failed");
    }

    [Fact]
    public async Task Dataflow_source_can_be_exposed_as_observable()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var source = new MqttDataflowSource<int>(FiniteSequence(), cancellationToken: timeout.Token);
        var observer = new RecordingObserver<int>();
        using var subscription = DataflowBlock.AsObservable(source).Subscribe(observer);

        (await observer.Next.Task.WaitAsync(timeout.Token)).ShouldBe(1);
        await source.Completion.WaitAsync(timeout.Token);
    }

    [Fact]
    public async Task Route_source_blocks_do_not_send_broker_subscribe_packets()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = new ResilientMqttClient(broker, NewOptions("local-route-only"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await using var source = client.ToRouteSourceBlock("local/{id}", cancellationToken: timeout.Token);
        await broker.PublishAsync(new MqttPublishPacket { Topic = "local/1" }, timeout.Token);

        await Should.ThrowAsync<TimeoutException>(
            source.ReceiveAsync(timeout.Token).WaitAsync(TimeSpan.FromMilliseconds(150), timeout.Token));
    }

    private static ResilientMqttClientOptions NewOptions(string clientId) => new()
    {
        Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
    };

    private static async Task WaitForStateAsync(
        ResilientMqttClient client,
        ConnectionState state,
        CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    private static async Task EventuallyAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    private static async IAsyncEnumerable<int> SlowSequence(
        TaskCompletionSource yieldedFirst,
        TaskCompletionSource yieldedSecond,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return 1;
        yieldedFirst.TrySetResult();
        yield return 2;
        yieldedSecond.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }

    private static async IAsyncEnumerable<int> FaultingSequence()
    {
        await Task.Yield();
        throw new InvalidOperationException("source failed");
        #pragma warning disable CS0162
        yield return 0;
        #pragma warning restore CS0162
    }

    private static async IAsyncEnumerable<int> FiniteSequence()
    {
        await Task.Yield();
        yield return 1;
    }

    private sealed class RecordingObserver<T> : IObserver<T>
    {
        public TaskCompletionSource<T> Next { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(T value)
        {
            Next.TrySetResult(value);
        }
    }
}
