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
    public async Task Manual_acknowledgement_endpoint_waits_for_context_ack()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        var transport = new WireTransportFactory();
        await using var client = new ResilientMqttClient(
            transport,
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket
                {
                    ClientId = "ep-manual-wire",
                    KeepAliveSeconds = 0,
                },
            });
        await client.ConnectAsync(timeout.Token);
        var broker = await transport.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new TaskCompletionSource<MqttEndpointContext>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt(
            "orders/{id}",
            async context =>
            {
                received.TrySetResult(context);
                await release.Task.WaitAsync(context.CancellationToken);
                await context.AcknowledgeAsync(context.CancellationToken);
            },
            new MqttEndpointOptions
            {
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                Acknowledgement = MqttAcknowledgementMode.Manual,
            });

        var subscribe = (await broker.ReadPacketAsync(timeout.Token))
            .ShouldBeOfTypeOrThrow<MqttSubscribePacket>();
        subscribe.TopicFilters.ShouldHaveSingleItem().ShouldSatisfyAllConditions(
            filter => filter.Topic.ShouldBe("orders/+"),
            filter => filter.MaximumQualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce));
        await broker.SendAsync(
            new MqttSubAckPacket
            {
                PacketIdentifier = subscribe.PacketIdentifier,
                ReasonCodes = [MqttReasonCode.GrantedQualityOfService1],
            },
            timeout.Token);
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await broker.SendAsync(
            new MqttPublishPacket
            {
                Topic = "orders/77",
                QualityOfService = MqttQualityOfService.AtLeastOnce,
                PacketIdentifier = 77,
            },
            timeout.Token);

        var context = await received.Task.WaitAsync(SafetyTimeout);
        context.Acknowledgement.ShouldBe(MqttAcknowledgementMode.Manual);
        context.IsAcknowledged.ShouldBeFalse();
        context.CanReject.ShouldBeTrue();

        var ackRead = Task.Run(() => broker.ReadPacketAsync(timeout.Token), CancellationToken.None);
        (await Task.WhenAny(ackRead, Task.Delay(100, timeout.Token))).ShouldNotBe(ackRead);

        release.SetResult();
        var ack = (await ackRead.WaitAsync(SafetyTimeout)).ShouldBeOfTypeOrThrow<MqttPublishAckPacket>();
        ack.PacketIdentifier.ShouldBe((ushort)77);
        context.IsAcknowledged.ShouldBeTrue();
    }

    [Fact]
    public async Task Manual_acknowledgement_typed_endpoint_deserializes_payload()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker, withSerializer: true);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var received = new TaskCompletionSource<(Reading Reading, int Device, MqttAcknowledgementMode Mode)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt<Reading>(
            "sensors/{deviceId:int}/manual-reading",
            async (reading, context) =>
            {
                received.TrySetResult((reading, context.Route.GetInt("deviceId"), context.Acknowledgement));
                await context.AcknowledgeAsync(context.CancellationToken);
            },
            new MqttEndpointOptions { Acknowledgement = MqttAcknowledgementMode.Manual });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker, withSerializer: true);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            "sensors/12/manual-reading",
            new Reading("sht31", 19.75),
            MqttQualityOfService.AtLeastOnce,
            cancellationToken: timeout.Token);

        var (reading, device, mode) = await received.Task.WaitAsync(timeout.Token);
        reading.ShouldBe(new Reading("sht31", 19.75));
        device.ShouldBe(12);
        mode.ShouldBe(MqttAcknowledgementMode.Manual);
    }

    [Fact]
    public async Task Automatic_endpoint_acknowledgement_methods_throw()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        var failure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var endpoint = client.MapMqtt("orders/{id}", async context =>
        {
            context.Acknowledgement.ShouldBe(MqttAcknowledgementMode.Automatic);
            context.IsAcknowledged.ShouldBeTrue();
            context.CanReject.ShouldBeFalse();

            try
            {
                await context.AcknowledgeAsync(context.CancellationToken);
            }
            catch (Exception error)
            {
                failure.TrySetResult(error);
            }
        });
        await endpoint.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket
            {
                Topic = "orders/auto",
                Payload = "ok"u8.ToArray(),
                QualityOfService = MqttQualityOfService.AtLeastOnce,
            },
            timeout.Token);

        var error = await failure.Task.WaitAsync(timeout.Token);
        error.ShouldBeOfType<InvalidOperationException>();
        error.Message.ShouldContain("automatic acknowledgement");
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

    [Fact]
    public async Task Disposing_one_endpoint_keeps_a_shared_filter_alive()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        // Distinct templates, one broker filter (alerts/+): disposing either endpoint must not
        // starve the other.
        var survivorSeen = new SemaphoreSlim(0);
        var survivorCount = 0;
        var survivor = client.MapMqtt("alerts/{level}", _ =>
        {
            Interlocked.Increment(ref survivorCount);
            survivorSeen.Release();
            return ValueTask.CompletedTask;
        });
        var disposed = client.MapMqtt("alerts/{severity}", _ => ValueTask.CompletedTask);
        await survivor.Subscribed.WaitAsync(timeout.Token);
        await disposed.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/red", Payload = "x"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await survivorSeen.WaitAsync(timeout.Token);

        await disposed.DisposeAsync();

        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/orange", Payload = "y"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await survivorSeen.WaitAsync(timeout.Token); // starved forever before the refcount fix

        // The last endpoint's disposal still releases the broker subscription.
        await survivor.DisposeAsync();
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/black", Payload = "z"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await Task.Delay(200, timeout.Token);

        survivorCount.ShouldBe(2);
    }

    [Fact]
    public async Task Disposing_one_endpoint_twice_keeps_a_shared_filter_alive()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await using var broker = new PulseMqttTestBroker();
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        // Distinct templates, one broker filter (alerts/+): the shared filter has two references.
        var survivorSeen = new SemaphoreSlim(0);
        var survivorCount = 0;
        var survivor = client.MapMqtt("alerts/{level}", _ =>
        {
            Interlocked.Increment(ref survivorCount);
            survivorSeen.Release();
            return ValueTask.CompletedTask;
        });
        var disposed = client.MapMqtt("alerts/{severity}", _ => ValueTask.CompletedTask);
        await survivor.Subscribed.WaitAsync(timeout.Token);
        await disposed.Subscribed.WaitAsync(timeout.Token);

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        // Dispose the SAME endpoint twice. A second release would drop the shared filter's refcount
        // to zero and unsubscribe alerts/+, starving the survivor. The idempotency guard must make
        // the second dispose a no-op; before the fix the survivor read below timed out.
        await disposed.DisposeAsync();
        await disposed.DisposeAsync();

        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/orange", Payload = "y"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await survivorSeen.WaitAsync(timeout.Token);

        survivorCount.ShouldBe(1);
    }

    [Fact]
    public async Task Disposing_a_denied_endpoint_does_not_unsubscribe_a_granted_shared_filter()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        // Grant the first SUBSCRIBE (the survivor) and deny every later one (the denied endpoint).
        var subscribeCount = 0;
        await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
        {
            SubAckFactory = context => Interlocked.Increment(ref subscribeCount) == 1
                ? context.DefaultSubAck
                : context.DefaultSubAck with
                {
                    ReasonCodes = [.. context.Subscribe.TopicFilters.Select(_ => MqttReasonCode.NotAuthorized)],
                },
        });
        await using var client = NewClient(broker);
        await client.ConnectAsync(timeout.Token);
        await client.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);

        // Two distinct templates, one broker filter (alerts/+). The survivor is granted; a second
        // endpoint on the same filter is denied and takes no reference. Disposing the denied
        // endpoint must not release the survivor's reference and unsubscribe alerts/+.
        var survivorSeen = new SemaphoreSlim(0);
        var survivorCount = 0;
        var survivor = client.MapMqtt("alerts/{level}", _ =>
        {
            Interlocked.Increment(ref survivorCount);
            survivorSeen.Release();
            return ValueTask.CompletedTask;
        });
        await survivor.Subscribed.WaitAsync(timeout.Token);

        var denied = client.MapMqtt("alerts/{severity}", _ => ValueTask.CompletedTask);
        await Should.ThrowAsync<MqttException>(() => denied.Subscribed.WaitAsync(timeout.Token));

        await using var publisher = NewClient(broker);
        await publisher.ConnectAsync(timeout.Token);
        await publisher.WaitUntilConnectedAsync(SafetyTimeout, timeout.Token);
        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/red", Payload = "x"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await survivorSeen.WaitAsync(timeout.Token);

        await denied.DisposeAsync(); // must NOT unsubscribe alerts/+ — the denied endpoint holds no reference

        await publisher.PublishAsync(
            new MqttPublishPacket { Topic = "alerts/orange", Payload = "y"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce }, timeout.Token);
        await survivorSeen.WaitAsync(timeout.Token); // starved forever before the fix

        survivorCount.ShouldBe(2);
        await survivor.DisposeAsync();
    }

    [Fact]
    public async Task A_denied_subscription_does_not_raise_unobserved_exceptions()
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

        var unobserved = 0;
        EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, args) =>
        {
            if (args.Exception.InnerExceptions.Any(inner => inner is MqttException m && m.Message.Contains("unobserved-secret")))
            {
                Interlocked.Increment(ref unobserved);
            }
        };
        TaskScheduler.UnobservedTaskException += handler;
        try
        {
            await MapAndAbandonDeniedEndpointAsync(client, timeout.Token);
            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            unobserved.ShouldBe(0);
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= handler;
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static async Task MapAndAbandonDeniedEndpointAsync(ResilientMqttClient client, CancellationToken token)
    {
        // Awaiting Subscribed is documented as optional: fault the task without ever observing
        // it, then abandon the endpoint so finalization would surface an unobserved exception.
        var endpoint = client.MapMqtt("unobserved-secret/{area}", _ => ValueTask.CompletedTask);
        while (!endpoint.Subscribed.IsFaulted)
        {
            token.ThrowIfCancellationRequested();
            await Task.Delay(10, token);
        }
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
