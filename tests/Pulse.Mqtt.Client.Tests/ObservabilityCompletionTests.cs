using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Serialization.Json;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

// Covers the N3 (connect/receive spans, duration histograms, offline-queue gauges) and N4 (W3C
// trace-context propagation) additions. The activity source and meter are process-global and other
// test classes run in parallel, so every test filters by a unique client id or topic.
public sealed class ObservabilityCompletionTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_received_message_runs_under_a_consumer_span()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        Activity? handlerActivity = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        router.RegisterRoute("metrics/{id}", (_, _, _) =>
        {
            handlerActivity = Activity.Current;
            handled.TrySetResult();
            return ValueTask.CompletedTask;
        });

        source.Writer.TryWrite(new MqttPublishPacket { Topic = "metrics/7" });

        await handled.Task.WaitAsync(SafetyTimeout);
        handlerActivity.ShouldNotBeNull();
        handlerActivity!.Kind.ShouldBe(ActivityKind.Consumer);
        handlerActivity.DisplayName.ShouldBe("receive metrics/7");
        handlerActivity.GetTagItem("messaging.system").ShouldBe("mqtt");
        handlerActivity.GetTagItem("messaging.destination.name").ShouldBe("metrics/7");
        handlerActivity.HasRemoteParent.ShouldBeFalse();
    }

    [Fact]
    public async Task A_receive_span_parents_on_an_incoming_traceparent()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        Activity? handlerActivity = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        router.RegisterRoute("trace/{id}", (_, _, _) =>
        {
            handlerActivity = Activity.Current;
            handled.TrySetResult();
            return ValueTask.CompletedTask;
        });

        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        source.Writer.TryWrite(new MqttPublishPacket
        {
            Topic = "trace/7",
            UserProperties = [new MqttUserProperty("traceparent", $"00-{traceId}-{spanId}-01")],
        });

        await handled.Task.WaitAsync(SafetyTimeout);
        handlerActivity.ShouldNotBeNull();
        handlerActivity!.TraceId.ShouldBe(traceId);
        handlerActivity.ParentSpanId.ShouldBe(spanId);
        handlerActivity.HasRemoteParent.ShouldBeTrue();
    }

    [Fact]
    public async Task A_receive_span_does_not_inherit_a_registration_time_activity()
    {
        var (source, router) = NewRouter();
        await using var _ = router;
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        Activity? handlerActivity = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Registering the route inside an active span makes that span the consumer task's ambient
        // Activity. A receive span with no incoming traceparent must still be a fresh root, not a
        // child of this unrelated registration-time span.
        using var ambient = PulseMqttDiagnostics.ActivitySource.StartActivity("ambient");
        ambient.ShouldNotBeNull();
        router.RegisterRoute("noparent/{id}", (_, _, _) =>
        {
            handlerActivity = Activity.Current;
            handled.TrySetResult();
            return ValueTask.CompletedTask;
        });

        source.Writer.TryWrite(new MqttPublishPacket { Topic = "noparent/1" });

        await handled.Task.WaitAsync(SafetyTimeout);
        handlerActivity.ShouldNotBeNull();
        handlerActivity!.HasRemoteParent.ShouldBeFalse();
        handlerActivity.Parent.ShouldBeNull();
        handlerActivity.TraceId.ShouldNotBe(ambient!.TraceId);
    }

    [Fact]
    public async Task Publish_injects_the_active_trace_context_when_propagation_is_enabled()
    {
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        var clientId = $"trace-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
            PropagateTraceContext = true,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        var topic = $"trace/{Guid.NewGuid():N}";
        var publish = client.PublishAsync(
            new MqttPublishPacket { Topic = topic, QualityOfService = MqttQualityOfService.AtMostOnce },
            timeout.Token);
        var sent = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await publish;

        var traceParent = sent.UserProperties.SingleOrDefault(p => p.Name == "traceparent");
        traceParent.ShouldNotBeNull();

        Activity publishActivity;
        lock (activities)
        {
            publishActivity = activities.Single(a => a.DisplayName == $"publish {topic}");
        }

        var flags = (publishActivity.ActivityTraceFlags & ActivityTraceFlags.Recorded) != 0 ? "01" : "00";
        traceParent!.Value.ShouldBe($"00-{publishActivity.TraceId}-{publishActivity.SpanId}-{flags}");
    }

    [Fact]
    public async Task Publish_omits_trace_context_by_default()
    {
        using var listener = NewActivityListener([]);

        var clientId = $"trace-off-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        var publish = client.PublishAsync(
            new MqttPublishPacket { Topic = $"trace/{Guid.NewGuid():N}", QualityOfService = MqttQualityOfService.AtMostOnce },
            timeout.Token);
        var sent = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await publish;

        sent.UserProperties.ShouldNotContain(p => p.Name == "traceparent");
    }

    [Fact]
    public async Task Publish_does_not_write_trace_context_user_properties_for_mqtt_3_1_1()
    {
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        var clientId = $"trace-v311-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = clientId,
                KeepAliveSeconds = 0,
                ProtocolVersion = MqttProtocolVersion.V311,
            },
            PropagateTraceContext = true,
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        var publish = client.PublishAsync(
            new MqttPublishPacket { Topic = $"trace/{Guid.NewGuid():N}", QualityOfService = MqttQualityOfService.AtMostOnce },
            timeout.Token);
        var sent = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await publish;

        sent.ProtocolVersion.ShouldBe(MqttProtocolVersion.V311);
        sent.UserProperties.ShouldBeEmpty();
    }

    [Fact]
    public async Task Trace_envelope_publish_captures_the_publish_span_context()
    {
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        var clientId = $"trace-envelope-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = clientId,
                KeepAliveSeconds = 0,
                ProtocolVersion = MqttProtocolVersion.V311,
            },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        var topic = $"trace-envelope/{Guid.NewGuid():N}";
        var publish = client.PublishWithTraceEnvelopeAsync(
            topic,
            new TelemetryReading("d-1", 2.5),
            cancellationToken: timeout.Token);
        var sent = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        await publish;

        sent.ProtocolVersion.ShouldBe(MqttProtocolVersion.V311);
        sent.UserProperties.ShouldBeEmpty();

        var envelope = new JsonMqttSerializer(TestJsonContext.Default)
            .Deserialize<MqttTraceEnvelope<TelemetryReading>>(sent.Payload);
        envelope.Payload.ShouldBe(new TelemetryReading("d-1", 2.5));
        envelope.TraceParent.ShouldNotBeNull();

        Activity publishActivity;
        lock (activities)
        {
            publishActivity = activities.Single(a => a.DisplayName == $"publish {topic}");
        }

        var flags = (publishActivity.ActivityTraceFlags & ActivityTraceFlags.Recorded) != 0 ? "01" : "00";
        envelope.TraceParent.ShouldBe($"00-{publishActivity.TraceId}-{publishActivity.SpanId}-{flags}");
    }

    [Fact]
    public async Task Trace_envelope_route_runs_handler_under_the_remote_context()
    {
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        var clientId = $"trace-envelope-route-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
            Serializer = new JsonMqttSerializer(TestJsonContext.Default),
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        Activity? handlerActivity = null;
        var received = new TaskCompletionSource<TelemetryReading>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = client.RegisterTraceEnvelopeRoute<TelemetryReading>(
            MqttRouteTemplate.Parse("trace-envelope/{device}"),
            (reading, _, _) =>
            {
                handlerActivity = Activity.Current;
                received.TrySetResult(reading);
                return ValueTask.CompletedTask;
            });

        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        var envelope = new MqttTraceEnvelope<TelemetryReading>(
            new TelemetryReading("d-7", 7.5),
            $"00-{traceId}-{spanId}-01");

        await broker.SendAsync(
            new MqttPublishPacket
            {
                Topic = "trace-envelope/d-7",
                Payload = new JsonMqttSerializer(TestJsonContext.Default).Serialize(envelope),
            },
            timeout.Token);

        (await received.Task.WaitAsync(SafetyTimeout)).ShouldBe(new TelemetryReading("d-7", 7.5));
        handlerActivity.ShouldNotBeNull();
        handlerActivity!.TraceId.ShouldBe(traceId);
        handlerActivity.ParentSpanId.ShouldBe(spanId);
        handlerActivity.HasRemoteParent.ShouldBeTrue();
    }

    [Fact]
    public async Task Connecting_emits_a_client_connect_span()
    {
        var activities = new List<Activity>();
        using var listener = NewActivityListener(activities);

        var clientId = $"connect-span-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        Activity connect;
        lock (activities)
        {
            connect = activities.Single(a => a.DisplayName == "connect" && Equals(a.GetTagItem("client.id"), clientId));
        }

        connect.Kind.ShouldBe(ActivityKind.Client);
        connect.GetTagItem("messaging.system").ShouldBe("mqtt");
        connect.GetTagItem("pulse.mqtt.session_present").ShouldBe(false);
    }

    [Fact]
    public async Task Connect_and_publish_durations_are_recorded()
    {
        var measurements = new List<(string Instrument, double Value, Dictionary<string, object?> Tags)>();
        using var listener = NewDoubleMeterListener(measurements);

        var clientId = $"durations-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
        });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await WaitForConnectedAsync(client, timeout.Token);

        await client.PublishAsync(
            new MqttPublishPacket { Topic = $"durations/{Guid.NewGuid():N}", QualityOfService = MqttQualityOfService.AtMostOnce },
            timeout.Token);

        lock (measurements)
        {
            measurements.ShouldContain(m =>
                m.Instrument == "pulse.mqtt.client.connect.duration"
                && Equals(m.Tags["client.id"], clientId)
                && Equals(m.Tags["outcome"], "success"));
            measurements.ShouldContain(m =>
                m.Instrument == "pulse.mqtt.client.publish.duration"
                && Equals(m.Tags["client.id"], clientId)
                && Equals(m.Tags["disposition"], "Delivered"));
        }
    }

    [Fact]
    public async Task Offline_queue_depth_and_drops_are_observable()
    {
        var measurements = new List<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var listener = NewLongMeterListener(measurements);

        var clientId = $"queue-{Guid.NewGuid():N}";
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
            OfflineQueue = new OfflineQueueOptions { Capacity = 1, Overflow = OverflowPolicy.DropNewest },
        });

        // Offline (never started): the first publish queues, the next two overflow and are dropped.
        for (var i = 0; i < 3; i++)
        {
            await client.PublishAsync(
                new MqttPublishPacket { Topic = "queue/x", QualityOfService = MqttQualityOfService.AtLeastOnce },
                CancellationToken.None);
        }

        listener.RecordObservableInstruments();
        lock (measurements)
        {
            measurements
                .Single(m => m.Instrument == "pulse.mqtt.client.offline.queue.depth" && Equals(m.Tags["client.id"], clientId))
                .Value.ShouldBe(1);
            measurements
                .Single(m => m.Instrument == "pulse.mqtt.client.offline.queue.dropped" && Equals(m.Tags["client.id"], clientId))
                .Value.ShouldBe(2);
        }
    }

    [Fact]
    public async Task A_throwing_message_store_does_not_break_observation_for_other_clients()
    {
        var faultingId = $"faulting-{Guid.NewGuid():N}";
        var healthyId = $"healthy-{Guid.NewGuid():N}";

        await using var faulting = new ResilientMqttClient(new SequencedTransportFactory(), new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = faultingId, KeepAliveSeconds = 0 },
            MessageStore = new ThrowingMessageStore(),
        });
        await using var healthy = new ResilientMqttClient(new SequencedTransportFactory(), new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = healthyId, KeepAliveSeconds = 0 },
        });

        // The healthy client queues one message offline; the faulting client's store throws on read.
        await healthy.PublishAsync(
            new MqttPublishPacket { Topic = "x", QualityOfService = MqttQualityOfService.AtLeastOnce },
            CancellationToken.None);

        var measurements = new List<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var listener = NewLongMeterListener(measurements);

        // Must not throw despite the faulting probe, and the healthy client's measurement survives.
        listener.RecordObservableInstruments();
        lock (measurements)
        {
            measurements
                .Single(m => m.Instrument == "pulse.mqtt.client.offline.queue.depth" && Equals(m.Tags["client.id"], healthyId))
                .Value.ShouldBe(1);
            measurements.ShouldNotContain(m => Equals(m.Tags.GetValueOrDefault("client.id"), faultingId));
        }
    }

    [Fact]
    public async Task Disposing_a_client_unregisters_its_offline_queue_probe()
    {
        var clientId = $"unregister-{Guid.NewGuid():N}";
        var client = new ResilientMqttClient(new SequencedTransportFactory(), new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = clientId, KeepAliveSeconds = 0 },
        });
        await client.PublishAsync(
            new MqttPublishPacket { Topic = "x", QualityOfService = MqttQualityOfService.AtLeastOnce },
            CancellationToken.None);
        await client.DisposeAsync();

        var measurements = new List<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var listener = NewLongMeterListener(measurements);
        listener.RecordObservableInstruments();
        lock (measurements)
        {
            measurements.ShouldNotContain(m =>
                m.Instrument == "pulse.mqtt.client.offline.queue.depth" && Equals(m.Tags["client.id"], clientId));
        }
    }

    private sealed class ThrowingMessageStore : IMessageStore
    {
        public int Count => throw new InvalidOperationException("store unavailable");

        public long DroppedCount => throw new InvalidOperationException("store unavailable");

        public ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken) => ValueTask.FromResult<MqttPublishPacket?>(null);

        public ValueTask RemoveHeadAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ClearAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private static (Channel<MqttPublishPacket> Source, MqttRouter Router) NewRouter()
    {
        var source = Channel.CreateUnbounded<MqttPublishPacket>();
        var router = new MqttRouter(source.Reader);
        router.Start();
        return (source, router);
    }

    private static async Task WaitForConnectedAsync(ResilientMqttClient client, CancellationToken cancellationToken)
    {
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    private static ActivityListener NewActivityListener(List<Activity> activities)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PulseMqttDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (activities) { activities.Add(activity); } },
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static MeterListener NewDoubleMeterListener(List<(string, double, Dictionary<string, object?>)> measurements)
    {
        var listener = NewMeterListener();
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            var map = ToMap(tags);
            lock (measurements)
            {
                measurements.Add((instrument.Name, value, map));
            }
        });
        listener.Start();
        return listener;
    }

    private static MeterListener NewLongMeterListener(List<(string, long, Dictionary<string, object?>)> measurements)
    {
        var listener = NewMeterListener();
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var map = ToMap(tags);
            lock (measurements)
            {
                measurements.Add((instrument.Name, value, map));
            }
        });
        listener.Start();
        return listener;
    }

    private static MeterListener NewMeterListener() => new()
    {
        InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == PulseMqttDiagnostics.SourceName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        },
    };

    private static Dictionary<string, object?> ToMap(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var map = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            map[tag.Key] = tag.Value;
        }

        return map;
    }
}
