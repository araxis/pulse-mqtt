using System.Text;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Testing.Tests;

public sealed class PulseMqttTestBrokerTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan NoMessageTimeout = TimeSpan.FromMilliseconds(200);

    [Fact]
    public async Task Supports_mqtt_311_subscribe_publish_unsubscribe_and_disconnect()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var subscriber = new RawMqttClient(broker);
        await using var publisher = new RawMqttClient(broker);
        using var timeout = new CancellationTokenSource(SafetyTimeout);

        await subscriber.ConnectAsync(NewConnect("v3-sub", MqttProtocolVersion.V311), timeout.Token);
        await publisher.ConnectAsync(NewConnect("v3-pub", MqttProtocolVersion.V311), timeout.Token);

        var granted = await subscriber.SubscribeAsync(
            [new MqttTopicFilter("v3/topic") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            timeout.Token);
        granted.ShouldBe([MqttReasonCode.GrantedQualityOfService1]);

        await publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "v3/topic",
                Payload = "hello"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);

        var received = await subscriber.Messages.ReadAsync(timeout.Token);
        received.ProtocolVersion.ShouldBe(MqttProtocolVersion.V311);
        received.Topic.ShouldBe("v3/topic");
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("hello");

        var removed = await subscriber.UnsubscribeAsync(["v3/topic"], timeout.Token);
        removed.ShouldBeEmpty();

        await publisher.PublishAsync(new MqttPublishPacket { Topic = "v3/topic", Payload = "after"u8.ToArray() }, timeout.Token);
        (await TryReadAsync(subscriber)).ShouldBeNull();

        await subscriber.DisconnectAsync(timeout.Token);
        await publisher.DisconnectAsync(timeout.Token);
    }

    [Fact]
    public async Task Retained_messages_are_disabled_by_default()
    {
        await using var broker = new PulseMqttTestBroker();
        await broker.PublishAsync(Retained("retain/default", "current"));

        await using var client = await ConnectRawAsync(broker, "retained-default");
        await client.SubscribeAsync([new MqttTopicFilter("retain/default")], CancellationToken.None);

        (await TryReadAsync(client)).ShouldBeNull();
    }

    [Fact]
    public async Task Retained_messages_replay_to_late_subscribers_when_enabled()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions { RetainedMessages = true });
        await broker.PublishAsync(Retained("retain/replay", "current"));

        await using var client = await ConnectRawAsync(broker, "retained-replay");
        await client.SubscribeAsync(
            [new MqttTopicFilter("retain/replay") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }],
            CancellationToken.None);

        var received = await client.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        received.Topic.ShouldBe("retain/replay");
        received.Retain.ShouldBeTrue();
        received.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("current");
    }

    [Fact]
    public async Task Zero_length_retained_publish_clears_the_retained_entry()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions { RetainedMessages = true });
        await broker.PublishAsync(Retained("retain/clear", "current"));
        await broker.PublishAsync(new MqttPublishPacket { Topic = "retain/clear", Retain = true });

        await using var client = await ConnectRawAsync(broker, "retained-clear");
        await client.SubscribeAsync([new MqttTopicFilter("retain/clear")], CancellationToken.None);

        (await TryReadAsync(client)).ShouldBeNull();
    }

    [Fact]
    public async Task Retain_handling_controls_replay()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions { RetainedMessages = true });
        await broker.PublishAsync(Retained("retain/once", "current"));
        await broker.PublishAsync(Retained("retain/skip", "current"));

        await using var once = await ConnectRawAsync(broker, "retained-once");
        var onceFilter = new MqttTopicFilter("retain/once")
        {
            RetainHandling = MqttRetainHandling.SendAtSubscribeIfNewSubscription,
        };

        await once.SubscribeAsync([onceFilter], CancellationToken.None);
        (await once.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout)).Topic.ShouldBe("retain/once");

        await once.SubscribeAsync([onceFilter], CancellationToken.None);
        (await TryReadAsync(once)).ShouldBeNull();

        await using var skip = await ConnectRawAsync(broker, "retained-skip");
        await skip.SubscribeAsync(
            [new MqttTopicFilter("retain/skip") { RetainHandling = MqttRetainHandling.DoNotSendAtSubscribe }],
            CancellationToken.None);
        (await TryReadAsync(skip)).ShouldBeNull();
    }

    [Fact]
    public async Task No_local_and_retain_as_published_are_honored()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = await ConnectRawAsync(broker, "subscription-options");

        await client.SubscribeAsync(
            [new MqttTopicFilter("echo/no-local") { NoLocal = true, MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
            CancellationToken.None);
        await client.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "echo/no-local",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            CancellationToken.None);
        (await TryReadAsync(client)).ShouldBeNull();

        await client.SubscribeAsync(
            [
                new MqttTopicFilter("echo/retain-as-published")
                {
                    RetainAsPublished = true,
                    MaximumQualityOfService = MqttQualityOfService.AtLeastOnce,
                },
            ],
            CancellationToken.None);
        await client.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "echo/retain-as-published",
                Retain = true,
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            CancellationToken.None);

        var retainedEcho = await client.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        retainedEcho.Retain.ShouldBeTrue();
    }

    [Fact]
    public async Task Persistent_sessions_are_disabled_by_default()
    {
        await using var broker = new PulseMqttTestBroker();

        await using (var first = await ConnectRawAsync(broker, "default-session", cleanStart: false))
        {
            await first.SubscribeAsync([new MqttTopicFilter("session/default")], CancellationToken.None);
            await first.DisconnectAsync(CancellationToken.None);
        }

        await using var second = new RawMqttClient(broker);
        var connAck = await second.ConnectAsync(NewConnect("default-session", cleanStart: false), CancellationToken.None);
        connAck.SessionPresent.ShouldBeFalse();

        await broker.PublishAsync(new MqttPublishPacket { Topic = "session/default", Payload = "miss"u8.ToArray() });
        (await TryReadAsync(second)).ShouldBeNull();
    }

    [Fact]
    public async Task Persistent_sessions_restore_subscriptions_when_enabled()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions { PersistentSessions = true });

        await using (var first = await ConnectRawAsync(broker, "persistent-session", cleanStart: false))
        {
            await first.SubscribeAsync([new MqttTopicFilter("session/persistent")], CancellationToken.None);
            await first.DisconnectAsync(CancellationToken.None);
        }

        await using var second = new RawMqttClient(broker);
        var connAck = await second.ConnectAsync(NewConnect("persistent-session", cleanStart: false), CancellationToken.None);
        connAck.SessionPresent.ShouldBeTrue();

        await broker.PublishAsync(new MqttPublishPacket { Topic = "session/persistent", Payload = "hit"u8.ToArray() });
        var received = await second.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("hit");
    }

    [Fact]
    public async Task Clean_start_clears_persistent_session_state()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions { PersistentSessions = true });

        await using (var first = await ConnectRawAsync(broker, "clean-clear", cleanStart: false))
        {
            await first.SubscribeAsync([new MqttTopicFilter("session/clean-clear")], CancellationToken.None);
            await first.DisconnectAsync(CancellationToken.None);
        }

        await using var second = new RawMqttClient(broker);
        var connAck = await second.ConnectAsync(NewConnect("clean-clear", cleanStart: true), CancellationToken.None);
        connAck.SessionPresent.ShouldBeFalse();

        await broker.PublishAsync(new MqttPublishPacket { Topic = "session/clean-clear" });
        (await TryReadAsync(second)).ShouldBeNull();
    }

    [Fact]
    public async Task Zero_session_expiry_clears_persistent_session_state()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions { PersistentSessions = true });

        await using (var first = await ConnectRawAsync(broker, "expiry-clear", cleanStart: false))
        {
            await first.SubscribeAsync([new MqttTopicFilter("session/expiry-clear")], CancellationToken.None);
            await first.DisconnectAsync(CancellationToken.None);
        }

        await using var second = new RawMqttClient(broker);
        var connAck = await second.ConnectAsync(
            NewConnect("expiry-clear", cleanStart: false) with { SessionExpiryInterval = 0 },
            CancellationToken.None);
        connAck.SessionPresent.ShouldBeFalse();

        await broker.PublishAsync(new MqttPublishPacket { Topic = "session/expiry-clear" });
        (await TryReadAsync(second)).ShouldBeNull();
    }

    [Fact]
    public async Task Forwarded_qos_is_capped_at_qos1_by_default()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var client = await ConnectRawAsync(broker, "qos-default");
        await client.SubscribeAsync(
            [new MqttTopicFilter("qos/default") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }],
            CancellationToken.None);

        await broker.PublishAsync(
            new MqttPublishPacket { Topic = "qos/default", QualityOfService = MqttQualityOfService.ExactlyOnce });

        var received = await client.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        received.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
    }

    [Fact]
    public async Task Forwarded_qos2_is_opt_in()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            MaximumForwardQualityOfService = MqttQualityOfService.ExactlyOnce,
        });
        await using var client = await ConnectRawAsync(broker, "qos2-forward");
        await client.SubscribeAsync(
            [new MqttTopicFilter("qos/two") { MaximumQualityOfService = MqttQualityOfService.ExactlyOnce }],
            CancellationToken.None);

        await broker.PublishAsync(
            new MqttPublishPacket { Topic = "qos/two", QualityOfService = MqttQualityOfService.ExactlyOnce });

        var received = await client.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        received.QualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);
        await client.DisconnectAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ConnAck_factory_can_stamp_success_properties()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            ConnAckFactory = context =>
            {
                context.ClientId.ShouldBe("connack-success");
                context.ProtocolVersion.ShouldBe(MqttProtocolVersion.V500);
                context.SessionPresent.ShouldBeFalse();

                return context.DefaultConnAck with
                {
                    AssignedClientIdentifier = "assigned-connack-success",
                    ReceiveMaximum = 7,
                    ServerReference = "mqtt://alternate",
                };
            },
        });
        await using var client = new RawMqttClient(broker);

        var connAck = await client.ConnectAsync(NewConnect("connack-success"), CancellationToken.None)
            .WaitAsync(SafetyTimeout);

        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        connAck.ProtocolVersion.ShouldBe(MqttProtocolVersion.V500);
        connAck.AssignedClientIdentifier.ShouldBe("assigned-connack-success");
        connAck.ReceiveMaximum.ShouldBe((ushort)7);
        connAck.ServerReference.ShouldBe("mqtt://alternate");
    }

    [Fact]
    public async Task ConnAck_factory_can_reject_without_creating_persistent_session_state()
    {
        var reject = true;
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            PersistentSessions = true,
            ConnAckFactory = context => reject
                ? context.DefaultConnAck with
                {
                    ReasonCode = MqttReasonCode.NotAuthorized,
                    ReasonString = "blocked",
                }
                : context.DefaultConnAck,
        });

        await using (var rejected = new RawMqttClient(broker))
        {
            var connAck = await rejected.ConnectAsync(
                    NewConnect("rejected-session", cleanStart: false),
                    CancellationToken.None)
                .WaitAsync(SafetyTimeout);
            connAck.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
            await Should.ThrowAsync<InvalidOperationException>(() => rejected.SubscribeAsync(
                [new MqttTopicFilter("rejected/unused")],
                CancellationToken.None));
        }

        reject = false;
        await using var accepted = new RawMqttClient(broker);
        var resumed = await accepted.ConnectAsync(
                NewConnect("rejected-session", cleanStart: false),
                CancellationToken.None)
            .WaitAsync(SafetyTimeout);

        resumed.ReasonCode.ShouldBe(MqttReasonCode.Success);
        resumed.SessionPresent.ShouldBeFalse();
    }

    [Fact]
    public async Task SubAck_factory_denies_one_filter_while_allowing_another()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            SubAckFactory = context =>
            {
                context.ClientId.ShouldBe("suback-deny-one");

                return context.DefaultSubAck with
                {
                    ReasonCodes =
                    [
                        MqttReasonCode.NotAuthorized,
                        MqttReasonCode.GrantedQualityOfService1,
                    ],
                };
            },
        });
        await using var client = await ConnectRawAsync(broker, "suback-deny-one");

        var results = await client.SubscribeAsync(
            [
                new MqttTopicFilter("script/denied"),
                new MqttTopicFilter("script/allowed") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce },
            ],
            CancellationToken.None);
        results.ShouldBe([MqttReasonCode.NotAuthorized, MqttReasonCode.GrantedQualityOfService1]);

        await broker.PublishAsync(new MqttPublishPacket { Topic = "script/denied", Payload = "miss"u8.ToArray() });
        await broker.PublishAsync(new MqttPublishPacket { Topic = "script/allowed", Payload = "hit"u8.ToArray() });

        var received = await client.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        received.Topic.ShouldBe("script/allowed");
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("hit");
        (await TryReadAsync(client)).ShouldBeNull();
    }

    [Fact]
    public async Task Denied_subscriptions_are_not_retained_in_persistent_sessions()
    {
        var deny = true;
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            PersistentSessions = true,
            SubAckFactory = context => deny
                ? context.DefaultSubAck with { ReasonCodes = [MqttReasonCode.NotAuthorized] }
                : context.DefaultSubAck,
        });

        await using (var first = await ConnectRawAsync(broker, "denied-persistent", cleanStart: false))
        {
            var results = await first.SubscribeAsync([new MqttTopicFilter("session/denied")], CancellationToken.None);
            results.ShouldBe([MqttReasonCode.NotAuthorized]);
            await first.DisconnectAsync(CancellationToken.None);
        }

        deny = false;
        await using var second = new RawMqttClient(broker);
        var connAck = await second.ConnectAsync(
                NewConnect("denied-persistent", cleanStart: false),
                CancellationToken.None)
            .WaitAsync(SafetyTimeout);

        connAck.SessionPresent.ShouldBeTrue();
        await broker.PublishAsync(new MqttPublishPacket { Topic = "session/denied", Payload = "miss"u8.ToArray() });
        (await TryReadAsync(second)).ShouldBeNull();
    }

    [Fact]
    public async Task PublishAck_factory_failure_surfaces_to_publishers_and_does_not_route()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            PublishAckFactory = context =>
            {
                context.ClientId.ShouldBe("ack-failure-publisher");

                return context.DefaultAcknowledgement with
                {
                    ReasonCode = MqttReasonCode.NotAuthorized,
                    ReasonString = "publish denied",
                };
            },
        });
        await using var subscriber = await ConnectRawAsync(broker, "ack-failure-subscriber");
        await using var publisher = await ConnectRawAsync(broker, "ack-failure-publisher");

        await subscriber.SubscribeAsync([new MqttTopicFilter("ack/failure/#")], CancellationToken.None);

        var qos1 = await publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "ack/failure/qos1",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            CancellationToken.None);
        qos1.ShouldBe(MqttReasonCode.NotAuthorized);
        (await broker.ClientPublishes.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout))
            .Topic.ShouldBe("ack/failure/qos1");

        var qos2 = await publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "ack/failure/qos2",
                QualityOfService = MqttQualityOfService.ExactlyOnce,
            },
            CancellationToken.None);
        qos2.ShouldBe(MqttReasonCode.NotAuthorized);
        (await broker.ClientPublishes.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout))
            .Topic.ShouldBe("ack/failure/qos2");
        (await TryReadAsync(subscriber)).ShouldBeNull();
    }

    [Fact]
    public async Task PublishAck_factory_can_withhold_acknowledgement_without_killing_broker()
    {
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            PublishAckFactory = context => context.Publish.Topic == "ack/withheld"
                ? null
                : context.DefaultAcknowledgement,
        });
        await using var publisher = new RawMqttClient(
            broker,
            new RawMqttClientOptions { AcknowledgementTimeout = TimeSpan.FromMilliseconds(100) });

        await publisher.ConnectAsync(NewConnect("withheld-publisher"), CancellationToken.None)
            .WaitAsync(SafetyTimeout);

        await Should.ThrowAsync<MqttException>(() => publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "ack/withheld",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            CancellationToken.None));
        (await broker.ClientPublishes.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout))
            .Topic.ShouldBe("ack/withheld");

        await using var subscriber = await ConnectRawAsync(broker, "withheld-broker-still-routes");
        await subscriber.SubscribeAsync([new MqttTopicFilter("ack/after-timeout")], CancellationToken.None);
        await broker.PublishAsync(new MqttPublishPacket { Topic = "ack/after-timeout", Payload = "ok"u8.ToArray() });

        var received = await subscriber.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        received.Topic.ShouldBe("ack/after-timeout");
    }

    [Fact]
    public async Task DisconnectClientAsync_sends_disconnect_details_and_only_affects_matching_client_id()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var first = await ConnectRawAsync(broker, "disconnect-one");
        await using var second = await ConnectRawAsync(broker, "disconnect-two");

        await second.SubscribeAsync([new MqttTopicFilter("disconnect/still-connected")], CancellationToken.None);

        var disconnected = await broker.DisconnectClientAsync(
            "disconnect-one",
            new MqttDisconnectPacket
            {
                ReasonCode = MqttReasonCode.ServerMoved,
                ReasonString = "move",
                ServerReference = "mqtt://next",
            },
            CancellationToken.None);

        disconnected.ShouldBe(1);
        await first.Completion.WaitAsync(SafetyTimeout);
        first.ServerDisconnect.ShouldNotBeNull();
        first.ServerDisconnect.ReasonCode.ShouldBe(MqttReasonCode.ServerMoved);
        first.ServerDisconnect.ReasonString.ShouldBe("move");
        first.ServerDisconnect.ServerReference.ShouldBe("mqtt://next");

        await broker.PublishAsync(new MqttPublishPacket { Topic = "disconnect/still-connected", Payload = "still"u8.ToArray() });
        var received = await second.Messages.ReadAsync(CancellationToken.None).AsTask().WaitAsync(SafetyTimeout);
        Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("still");
    }

    [Fact]
    public async Task DisconnectAllAsync_drops_all_active_sessions()
    {
        await using var broker = new PulseMqttTestBroker();
        await using var first = await ConnectRawAsync(broker, "disconnect-all-one");
        await using var second = await ConnectRawAsync(broker, "disconnect-all-two");

        var disconnected = await broker.DisconnectAllAsync(
            new MqttDisconnectPacket { ReasonCode = MqttReasonCode.ServerShuttingDown },
            CancellationToken.None);

        disconnected.ShouldBe(2);
        await first.Completion.WaitAsync(SafetyTimeout);
        await second.Completion.WaitAsync(SafetyTimeout);
        first.ServerDisconnect.ShouldNotBeNull();
        first.ServerDisconnect.ReasonCode.ShouldBe(MqttReasonCode.ServerShuttingDown);
        second.ServerDisconnect.ShouldNotBeNull();
        second.ServerDisconnect.ReasonCode.ShouldBe(MqttReasonCode.ServerShuttingDown);
    }

    private static async Task<RawMqttClient> ConnectRawAsync(
        PulseMqttTestBroker broker,
        string clientId,
        bool cleanStart = true)
    {
        var client = new RawMqttClient(broker);
        await client.ConnectAsync(NewConnect(clientId, cleanStart: cleanStart), CancellationToken.None)
            .WaitAsync(SafetyTimeout);
        return client;
    }

    private static MqttConnectPacket NewConnect(
        string clientId,
        MqttProtocolVersion version = MqttProtocolVersion.V500,
        bool cleanStart = true) =>
        new()
        {
            ClientId = clientId,
            ProtocolVersion = version,
            CleanStart = cleanStart,
            KeepAliveSeconds = 0,
        };

    private static MqttPublishPacket Retained(string topic, string payload) => new()
    {
        Topic = topic,
        Payload = Encoding.UTF8.GetBytes(payload),
        QualityOfService = MqttQualityOfService.AtLeastOnce,
        Retain = true,
    };

    private static async Task<MqttPublishPacket?> TryReadAsync(RawMqttClient client)
    {
        using var timeout = new CancellationTokenSource(NoMessageTimeout);
        try
        {
            return await client.Messages.ReadAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }
}
