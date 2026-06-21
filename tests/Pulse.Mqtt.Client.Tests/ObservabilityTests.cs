using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class ObservabilityTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class RecordingLogger : ILogger
    {
        public List<(EventId Event, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Entries)
            {
                Entries.Add((eventId, formatter(state, exception)));
            }
        }
    }

    private static ResilientMqttClientOptions NewOptions(ILogger? logger = null) => new()
    {
        Connect = new MqttConnectPacket { ClientId = "observed", KeepAliveSeconds = 0 },
        Logger = logger,
    };

    [Fact]
    public async Task Offline_publish_records_a_queued_measurement()
    {
        var measurements = new List<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var listener = NewMeterListener(measurements);

        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        await client.PublishAsync(
            new MqttPublishPacket { Topic = "t", QualityOfService = MqttQualityOfService.AtLeastOnce },
            CancellationToken.None);

        listener.RecordObservableInstruments();
        lock (measurements)
        {
            // The meter is process-global and other test classes publish in parallel; filter
            // to this test's client id.
            var published = measurements
                .Where(m => m.Instrument == "pulse.mqtt.client.messages.published"
                    && Equals(m.Tags["client.id"], "observed"))
                .ToList();
            var entry = published.ShouldHaveSingleItem();
            entry.Value.ShouldBe(1);
            entry.Tags["disposition"].ShouldBe("Queued");
        }
    }

    [Fact]
    public async Task Connecting_records_attempt_measurements()
    {
        var measurements = new List<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var listener = NewMeterListener(measurements);

        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        lock (measurements)
        {
            measurements.Count(m => m.Instrument == "pulse.mqtt.client.connect.attempts").ShouldBeGreaterThanOrEqualTo(1);
            measurements.Count(m => m.Instrument == "pulse.mqtt.client.state.transitions").ShouldBeGreaterThanOrEqualTo(2);
        }
    }

    [Fact]
    public async Task Publish_emits_a_producer_activity_with_messaging_tags()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PulseMqttDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (activities) { activities.Add(activity); } },
        };
        ActivitySource.AddActivityListener(listener);

        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        // The listener is process-global and other test classes publish in parallel, so the
        // topic is unique and the assertions filter to it.
        var topic = $"observed/{Guid.NewGuid():N}";
        await client.PublishAsync(
            new MqttPublishPacket { Topic = topic, QualityOfService = MqttQualityOfService.AtLeastOnce },
            CancellationToken.None);

        lock (activities)
        {
            var activity = activities.Where(a => a.DisplayName == $"publish {topic}").ShouldHaveSingleItem();
            activity.Kind.ShouldBe(ActivityKind.Producer);
            activity.GetTagItem("messaging.system").ShouldBe("mqtt");
            activity.GetTagItem("messaging.destination.name").ShouldBe(topic);
            activity.GetTagItem("pulse.mqtt.disposition").ShouldBe("Queued");
        }
    }

    [Fact]
    public async Task State_changes_are_logged()
    {
        var logger = new RecordingLogger();
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(logger));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        while (client.State != ConnectionState.Connected)
        {
            await Task.Delay(5, timeout.Token);
        }

        lock (logger.Entries)
        {
            logger.Entries.ShouldContain(e => e.Event.Id == 1 && e.Message.Contains("Connecting"));
            logger.Entries.ShouldContain(e => e.Event.Id == 1 && e.Message.Contains("Connected"));
            logger.Entries.All(e => e.Message.Contains("observed")).ShouldBeTrue();
        }
    }

    private static MeterListener NewMeterListener(List<(string, long, Dictionary<string, object?>)> measurements)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == PulseMqttDiagnostics.SourceName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var tagMap = new Dictionary<string, object?>();
            foreach (var tag in tags)
            {
                tagMap[tag.Key] = tag.Value;
            }

            lock (measurements)
            {
                measurements.Add((instrument.Name, value, tagMap));
            }
        });
        listener.Start();
        return listener;
    }
}
