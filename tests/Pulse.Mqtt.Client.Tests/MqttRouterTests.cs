using System.Threading.Channels;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class MqttRouterTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private static (Channel<MqttPublishPacket> Source, MqttRouter Router) NewRouter()
    {
        var source = Channel.CreateUnbounded<MqttPublishPacket>();
        var router = new MqttRouter(source.Reader);
        router.Start();
        return (source, router);
    }

    private static MqttPublishPacket Message(string topic) => new() { Topic = topic };

    [Fact]
    public async Task Routes_capture_template_parameters()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var received = new TaskCompletionSource<(string Topic, string Id)>(TaskCreationOptions.RunContinuationsAsynchronously);

        router.RegisterRoute("sensors/{id}/temp", (message, values, _) =>
        {
            received.TrySetResult((message.Topic, values["id"]));
            return ValueTask.CompletedTask;
        });

        source.Writer.TryWrite(Message("sensors/7/temp"));

        var (topic, id) = await received.Task.WaitAsync(SafetyTimeout);
        topic.ShouldBe("sensors/7/temp");
        id.ShouldBe("7");
    }

    [Fact]
    public async Task Overlapping_routes_both_receive()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        router.RegisterRoute("a/#", (_, _, _) => { first.TrySetResult(); return ValueTask.CompletedTask; });
        router.RegisterRoute("a/+", (_, _, _) => { second.TrySetResult(); return ValueTask.CompletedTask; });

        source.Writer.TryWrite(Message("a/b"));

        await Task.WhenAll(first.Task, second.Task).WaitAsync(SafetyTimeout);
    }

    [Fact]
    public async Task Unmatched_messages_surface_on_the_unmatched_reader()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        router.RegisterRoute("known/#", (_, _, _) => ValueTask.CompletedTask);

        source.Writer.TryWrite(Message("other/topic"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        (await router.Unmatched.ReadAsync(timeout.Token)).Topic.ShouldBe("other/topic");
    }

    [Fact]
    public async Task A_slow_route_does_not_block_a_fast_route()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fastDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        router.RegisterRoute("slow/#", async (_, _, _) => await gate.Task);
        router.RegisterRoute("fast/#", (_, _, _) => { fastDone.TrySetResult(); return ValueTask.CompletedTask; });

        source.Writer.TryWrite(Message("slow/1"));
        source.Writer.TryWrite(Message("fast/1"));

        await fastDone.Task.WaitAsync(SafetyTimeout); // completes while the slow handler is still gated
        gate.TrySetResult();
    }

    [Fact]
    public async Task A_faulting_handler_surfaces_and_dispatch_continues()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var faulted = new TaskCompletionSource<(string Template, Exception Error)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = true;

        router.HandlerFaulted += (template, error) => faulted.TrySetResult((template, error));
        router.RegisterRoute("t/#", (_, _, _) =>
        {
            if (first)
            {
                first = false;
                throw new InvalidOperationException("boom");
            }

            delivered.TrySetResult();
            return ValueTask.CompletedTask;
        });

        source.Writer.TryWrite(Message("t/1"));
        source.Writer.TryWrite(Message("t/2"));

        var (template, error) = await faulted.Task.WaitAsync(SafetyTimeout);
        template.ShouldBe("t/#");
        error.ShouldBeOfType<InvalidOperationException>();
        await delivered.Task.WaitAsync(SafetyTimeout);
    }

    [Fact]
    public async Task A_disposed_registration_stops_delivery()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var registration = router.RegisterRoute("gone/#", (_, _, _) => ValueTask.CompletedTask);

        registration.Dispose();
        source.Writer.TryWrite(Message("gone/1"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        (await router.Unmatched.ReadAsync(timeout.Token)).Topic.ShouldBe("gone/1");
    }

    [Fact]
    public async Task Drop_oldest_keeps_the_latest_message()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        await using var stream = router.OpenRouteStream("s/#", new MqttRouteOptions
        {
            Capacity = 1,
            Overflow = RouteOverflow.DropOldest,
        });

        source.Writer.TryWrite(Message("s/1"));
        source.Writer.TryWrite(Message("s/2"));
        source.Writer.TryWrite(Message("s/3"));
        await Task.Delay(100); // let dispatch drain the source

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        (await stream.Reader.ReadAsync(timeout.Token)).Message.Topic.ShouldBe("s/3");
    }

    [Fact]
    public async Task Streams_yield_routed_messages()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        await using var stream = router.OpenRouteStream("n/{x}");

        source.Writer.TryWrite(Message("n/42"));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await foreach (var routed in stream.ReadAllAsync(timeout.Token))
        {
            routed.Values["x"].ShouldBe("42");
            break;
        }
    }

    [Fact]
    public async Task Client_routes_after_explicit_subscription()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "router", KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        // Registering the route after connection-up keeps it to exactly one SUBSCRIBE packet.
        while (client.State != Pulse.Mqtt.Resilience.ConnectionState.Connected)
        {
            await Task.Delay(1, timeout.Token);
        }

        var template = MqttRouteTemplate.Parse("sensors/{id}");
        var subscribeTask = client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], timeout.Token);

        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().Topic.ShouldBe("sensors/+");
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] },
            timeout.Token);
        await subscribeTask;

        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = client.RegisterRoute(template, (_, values, _) =>
        {
            received.TrySetResult(values["id"]);
            return ValueTask.CompletedTask;
        });

        await broker.SendAsync(Message("sensors/9"), timeout.Token);

        (await received.Task.WaitAsync(SafetyTimeout)).ShouldBe("9");
    }

    [Fact]
    public async Task Route_template_creates_subscription_filter_with_explicit_options()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "router-options", KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.StartAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var template = MqttRouteTemplate.Parse("sensors/{id}");
        var subscribeTask = client.SubscribeAsync([
            template.ToTopicFilter(
                MqttQualityOfService.ExactlyOnce,
                noLocal: true,
                retainAsPublished: true,
                retainHandling: MqttRetainHandling.DoNotSendAtSubscribe),
        ], timeout.Token);

        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        var filter = subscribe.TopicFilters.ShouldHaveSingleItem();
        filter.Topic.ShouldBe("sensors/+");
        filter.MaximumQualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);
        filter.NoLocal.ShouldBeTrue();
        filter.RetainAsPublished.ShouldBeTrue();
        filter.RetainHandling.ShouldBe(MqttRetainHandling.DoNotSendAtSubscribe);

        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService2] },
            timeout.Token);
        await subscribeTask;
    }
}
