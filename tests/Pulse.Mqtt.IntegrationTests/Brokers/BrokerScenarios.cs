using System.Globalization;
using System.Text;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Transport;
using Shouldly;

namespace Pulse.Mqtt.IntegrationTests.Brokers;

/// <summary>
/// The shared conformance scenarios run against every broker in the matrix. Each takes a broker
/// and exercises one capability through the raw client, so a failure names the broker and the
/// capability.
/// </summary>
public static class BrokerScenarios
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    public static async Task HandshakeAsync(IMqttBroker broker)
    {
        using var cancellation = new CancellationTokenSource(Timeout);
        await using var client = Connect(broker);

        var connAck = await client.ConnectAsync(NewConnect(), cancellation.Token);

        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        await client.DisconnectAsync(cancellation.Token);
    }

    public static async Task RoundTripAsync(IMqttBroker broker, MqttQualityOfService qos)
    {
        using var cancellation = new CancellationTokenSource(Timeout);
        var topic = $"matrix/{Guid.NewGuid():N}";

        await using var subscriber = Connect(broker);
        await subscriber.ConnectAsync(NewConnect(), cancellation.Token);
        await subscriber.SubscribeAsync([new MqttTopicFilter(topic) { MaximumQualityOfService = qos }], cancellation.Token);

        await using var publisher = Connect(broker);
        await publisher.ConnectAsync(NewConnect(), cancellation.Token);
        var payload = Encoding.UTF8.GetBytes($"payload-{qos}");
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = payload, QualityOfService = qos },
            cancellation.Token);

        var received = await subscriber.Messages.ReadAsync(cancellation.Token);
        received.Topic.ShouldBe(topic);
        received.QualityOfService.ShouldBe(qos);
        received.Payload.ToArray().ShouldBe(payload);

        // The message is delivered once — in particular QoS 2 must not duplicate.
        await AssertNoFurtherMessageAsync(subscriber, cancellation.Token);
    }

    public static async Task RetainedMessageAsync(IMqttBroker broker)
    {
        using var cancellation = new CancellationTokenSource(Timeout);
        var topic = $"matrix/retained/{Guid.NewGuid():N}";

        await using var publisher = Connect(broker);
        await publisher.ConnectAsync(NewConnect(), cancellation.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = "retained"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce, Retain = true },
            cancellation.Token);

        // A subscriber that connects after the publish still receives the retained message.
        await using var subscriber = Connect(broker);
        await subscriber.ConnectAsync(NewConnect(), cancellation.Token);
        await subscriber.SubscribeAsync(
            [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            cancellation.Token);

        var received = await subscriber.Messages.ReadAsync(cancellation.Token);
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("retained");
        received.Retain.ShouldBeTrue();

        // Exactly one retained message is replayed, not a duplicate.
        await AssertNoFurtherMessageAsync(subscriber, cancellation.Token);
    }

    public static async Task LargePayloadAsync(IMqttBroker broker)
    {
        using var cancellation = new CancellationTokenSource(Timeout);
        var topic = $"matrix/large/{Guid.NewGuid():N}";
        var payload = new byte[64 * 1024];
        Random.Shared.NextBytes(payload);

        await using var subscriber = Connect(broker);
        await subscriber.ConnectAsync(NewConnect(), cancellation.Token);
        await subscriber.SubscribeAsync(
            [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            cancellation.Token);

        await using var publisher = Connect(broker);
        await publisher.ConnectAsync(NewConnect(), cancellation.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = payload, QualityOfService = MqttQualityOfService.AtLeastOnce },
            cancellation.Token);

        var received = await subscriber.Messages.ReadAsync(cancellation.Token);
        received.Payload.ToArray().ShouldBe(payload);
    }

    public static async Task SharedSubscriptionAsync(IMqttBroker broker)
    {
        using var cancellation = new CancellationTokenSource(Timeout);
        var topic = $"matrix/shared/{Guid.NewGuid():N}";
        var shared = MqttSharedSubscription.Format("workers", topic);
        const int messageCount = 10;

        await using var memberA = Connect(broker);
        await memberA.ConnectAsync(NewConnect(), cancellation.Token);
        await memberA.SubscribeAsync([new MqttTopicFilter(shared) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }], cancellation.Token);

        await using var memberB = Connect(broker);
        await memberB.ConnectAsync(NewConnect(), cancellation.Token);
        await memberB.SubscribeAsync([new MqttTopicFilter(shared) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }], cancellation.Token);

        await using var publisher = Connect(broker);
        await publisher.ConnectAsync(NewConnect(), cancellation.Token);
        for (var sequence = 0; sequence < messageCount; sequence++)
        {
            await publisher.PublishAsync(
                new MqttPublishPacket
                {
                    Topic = topic,
                    Payload = Encoding.UTF8.GetBytes(sequence.ToString(CultureInfo.InvariantCulture)),
                    QualityOfService = MqttQualityOfService.AtLeastOnce,
                },
                cancellation.Token);
        }

        // Every message must go to exactly one of the two members. Reading messageCount deliveries
        // across both and checking they are the sequence 0..N-1 exactly catches double-delivery
        // deterministically: a fan-out to both members would repeat a number (and drop another),
        // failing the assertion regardless of timing — no negative wait window needed.
        var received = new List<int>(messageCount);
        var readA = memberA.Messages.ReadAsync(cancellation.Token).AsTask();
        var readB = memberB.Messages.ReadAsync(cancellation.Token).AsTask();
        for (var i = 0; i < messageCount; i++)
        {
            var completed = await Task.WhenAny(readA, readB);
            received.Add(int.Parse(Encoding.UTF8.GetString((await completed).Payload.Span), CultureInfo.InvariantCulture));
            if (completed == readA)
            {
                readA = memberA.Messages.ReadAsync(cancellation.Token).AsTask();
            }
            else
            {
                readB = memberB.Messages.ReadAsync(cancellation.Token).AsTask();
            }
        }

        received.OrderBy(n => n).ShouldBe(Enumerable.Range(0, messageCount));

        // One read per member is left pending; observe both so disposing the clients does not
        // surface an unobserved channel-closed exception.
        Observe(readA);
        Observe(readB);
    }

    // Swallows the eventual fault of a deliberately-abandoned read task.
    private static void Observe(Task task) =>
        _ = task.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);

    // Asserts the subscriber receives no further message within a short window — catching an
    // immediate duplicate delivery (e.g. a QoS 2 dedup regression) that a single read would miss.
    private static async Task AssertNoFurtherMessageAsync(RawMqttClient subscriber, CancellationToken cancellationToken)
    {
        var next = subscriber.Messages.ReadAsync(cancellationToken).AsTask();
        var arrived = await Task.WhenAny(next, Task.Delay(500)) == next;
        arrived.ShouldBeFalse("the subscriber received an unexpected duplicate delivery");
        Observe(next);
    }

    public static async Task PersistentSessionResumesAsync(IMqttBroker broker)
    {
        using var cancellation = new CancellationTokenSource(Timeout);
        var clientId = $"matrix-session-{Guid.NewGuid():N}";
        var topic = $"matrix/session/{Guid.NewGuid():N}";

        // CleanStart = true with a non-zero expiry starts fresh but keeps the session alive after
        // the disconnect, so the broker still has it to resume.
        var persistent = new MqttConnectPacket
        {
            ClientId = clientId,
            KeepAliveSeconds = 30,
            CleanStart = true,
            SessionExpiryInterval = 300,
        };
        await using (var first = Connect(broker))
        {
            var connAck = await first.ConnectAsync(persistent, cancellation.Token);
            connAck.SessionPresent.ShouldBeFalse();
            await first.SubscribeAsync(
                [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
                cancellation.Token);
            await first.DisconnectAsync(cancellation.Token);
        }

        // Reconnecting with the same id and CleanStart = false resumes the session: the broker
        // reports it present and still routes the earlier subscription.
        await using var second = Connect(broker);
        var resumed = await second.ConnectAsync(
            persistent with { CleanStart = false },
            cancellation.Token);
        resumed.SessionPresent.ShouldBeTrue();

        await using var publisher = Connect(broker);
        await publisher.ConnectAsync(NewConnect(), cancellation.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = topic, Payload = "resumed"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
            cancellation.Token);

        Encoding.UTF8.GetString((await second.Messages.ReadAsync(cancellation.Token)).Payload.Span).ShouldBe("resumed");
    }

    private static RawMqttClient Connect(IMqttBroker broker) =>
        new(new TcpTransportFactory(new TcpTransportOptions { Host = broker.Host, Port = broker.Port }));

    private static MqttConnectPacket NewConnect() => new()
    {
        ClientId = $"matrix-{Guid.NewGuid():N}",
        KeepAliveSeconds = 30,
    };
}
