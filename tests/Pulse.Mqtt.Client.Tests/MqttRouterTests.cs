using System.Diagnostics.Metrics;
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
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        // Registering the route after connection-up keeps it to exactly one SUBSCRIBE packet.
        while (client.State != Pulse.Mqtt.Resilience.ConnectionState.Connected)
        {
            await Task.Delay(1, timeout.Token);
        }

        var template = MqttRouteTemplate.Parse("sensors/{id}");
        var subscribeTask = client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, timeout.Token);

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
    public async Task Acknowledged_route_stream_waits_for_consumer_ack()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "router-ack", KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var template = MqttRouteTemplate.Parse("sensors/{id}");
        await using var stream = client.OpenAcknowledgedRouteStream(template);
        var subscribeTask = client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, timeout.Token);

        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] },
            timeout.Token);
        await subscribeTask;

        await broker.SendAsync(
            new MqttPublishPacket
            {
                Topic = "sensors/9",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                PacketIdentifier = 42,
            },
            timeout.Token);

        var routed = await stream.Reader.ReadAsync(timeout.Token);
        routed.Message.Topic.ShouldBe("sensors/9");
        routed.Values["id"].ShouldBe("9");

        var ackRead = Task.Run(() => broker.ReadPacketAsync(timeout.Token), CancellationToken.None);
        (await Task.WhenAny(ackRead, Task.Delay(100, timeout.Token))).ShouldNotBe(ackRead);

        await routed.AcknowledgeAsync(timeout.Token);

        var ack = (await ackRead.WaitAsync(SafetyTimeout)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>();
        ack.PacketType.ShouldBe(Pulse.Mqtt.Codec.MqttPacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)42);
    }

    [Fact]
    public async Task Disposing_a_blocked_acknowledged_stream_does_not_fault_the_connection()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "router-ack-dispose", KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var template = MqttRouteTemplate.Parse("sensors/{id}");
        var stream = client.OpenAcknowledgedRouteStream(template, new MqttRouteOptions { Capacity = 1, Overflow = RouteOverflow.Wait });
        var subscribeTask = client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, timeout.Token);
        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, timeout.Token);
        await subscribeTask;

        // Fill the Capacity-1 stream (left unread), then a second message blocks the shared inbound
        // sink on the lossless delivery.
        await broker.SendAsync(
            new MqttPublishPacket { Topic = "sensors/9", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 42 }, timeout.Token);
        await broker.SendAsync(
            new MqttPublishPacket { Topic = "sensors/9", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 43 }, timeout.Token);
        await Task.Delay(150, timeout.Token); // let the sink fill the channel and block on the second

        // Disposing the stream completes its channel; the pending delivery's ChannelClosedException
        // must not escape into the sink and fault the connection. Before the fix the client dropped
        // the connection and reconnected.
        await stream.DisposeAsync();
        await Task.Delay(250, timeout.Token);

        client.State.ShouldBe(Pulse.Mqtt.Resilience.ConnectionState.Connected);
        factory.ConnectionsHandedOut.ShouldBe(1, "disposing an acknowledged stream must not reconnect the client");
    }

    [Fact]
    public void Default_acknowledged_route_capacity_tracks_the_receive_maximum()
    {
        // With a Receive Maximum advertised, the default capacity matches it, so the broker's
        // in-flight window throttles before an acknowledged route's queue can fill and block the sink.
        ResilientMqttClient.DefaultAcknowledgedRouteCapacity(100).ShouldBe(100);
        ResilientMqttClient.DefaultAcknowledgedRouteCapacity(ushort.MaxValue).ShouldBe(ushort.MaxValue);

        // With none advertised (or a protocol-invalid zero) no finite capacity is provably safe, so
        // fall back to the route default rather than pretending otherwise.
        ResilientMqttClient.DefaultAcknowledgedRouteCapacity(null).ShouldBe(new MqttRouteOptions().Capacity);
        ResilientMqttClient.DefaultAcknowledgedRouteCapacity(0).ShouldBe(new MqttRouteOptions().Capacity);
    }

    [Fact]
    public async Task A_full_acknowledged_route_reports_backpressure_when_it_blocks_the_sink()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "router-ack-backpressure", KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        // Capture the acknowledged-route backpressure counter for our route only.
        var backpressured = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == PulseMqttDiagnostics.SourceName &&
                    instrument.Name == "pulse.mqtt.client.route.acknowledged.backpressure")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag is { Key: "route", Value: string route })
                {
                    backpressured.TrySetResult(route);
                }
            }
        });
        listener.Start();

        var template = MqttRouteTemplate.Parse("bp/{id}");
        await using var stream = client.OpenAcknowledgedRouteStream(template, new MqttRouteOptions { Capacity = 1, Overflow = RouteOverflow.Wait });
        var subscribeTask = client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, timeout.Token);
        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] }, timeout.Token);
        await subscribeTask;

        // The first message fills the Capacity-1 queue (left unread); the second finds it full, so the
        // lossless sink must block on it. That block is exactly what the backpressure metric records.
        await broker.SendAsync(
            new MqttPublishPacket { Topic = "bp/9", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 42 }, timeout.Token);
        await broker.SendAsync(
            new MqttPublishPacket { Topic = "bp/9", QualityOfService = MqttQualityOfService.AtLeastOnce, PacketIdentifier = 43 }, timeout.Token);

        (await backpressured.Task.WaitAsync(SafetyTimeout)).ShouldBe("bp/{id}");
    }

    [Fact]
    public async Task Acknowledged_route_stream_rejects_lossy_overflow()
    {
        var (source, router) = NewRouter();
        await using var _ = router;

        var thrown = Should.Throw<ArgumentException>(() =>
            router.OpenAcknowledgedRouteStream("s/#", new MqttRouteOptions
            {
                Capacity = 1,
                Overflow = RouteOverflow.DropOldest,
            }));

        thrown.Message.ShouldContain(nameof(RouteOverflow.Wait));
        source.Writer.TryComplete();
    }

    [Fact]
    public async Task Overlapping_acknowledged_route_streams_use_first_match_only()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "router-ack-overlap", KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        await using var first = client.OpenAcknowledgedRouteStream("sensors/#");
        await using var second = client.OpenAcknowledgedRouteStream("sensors/{id}");
        var subscribeTask = client.SubscribeAsync(
            [new MqttTopicFilter("sensors/#") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            timeout.Token);

        var subscribe = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        await broker.SendAsync(
            new MqttSubAckPacket { PacketIdentifier = subscribe.PacketIdentifier, ReasonCodes = [MqttReasonCode.GrantedQualityOfService1] },
            timeout.Token);
        await subscribeTask;

        await broker.SendAsync(
            new MqttPublishPacket
            {
                Topic = "sensors/9",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                PacketIdentifier = 43,
            },
            timeout.Token);

        var routed = await first.Reader.ReadAsync(timeout.Token);
        routed.Message.Topic.ShouldBe("sensors/9");
        second.Reader.TryRead(out _).ShouldBeFalse();

        await routed.AcknowledgeAsync(timeout.Token);
        var ack = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>();
        ack.PacketType.ShouldBe(Pulse.Mqtt.Codec.MqttPacketType.PubAck);
        ack.PacketIdentifier.ShouldBe((ushort)43);
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
        await client.ConnectAsync(timeout.Token);
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
