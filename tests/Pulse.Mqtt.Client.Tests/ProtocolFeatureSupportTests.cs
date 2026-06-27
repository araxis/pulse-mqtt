using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

public sealed class ProtocolFeatureSupportTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private sealed class ZeroJitter : Random
    {
        public override double NextDouble() => 0.0;
    }

    [Fact]
    public async Task Disconnected_mqtt311_reports_mqtt5_features_as_not_supported()
    {
        await using var client = new ResilientMqttClient(
            new SequencedTransportFactory(),
            NewOptions(MqttProtocolVersion.V311));

        client.GetProtocolFeatureSupport(MqttProtocolFeature.RequestResponse)
            .ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        client.GetProtocolFeatureSupport(MqttProtocolFeature.TopicAliases)
            .ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        client.CanUseProtocolFeature(MqttProtocolFeature.RequestResponse).ShouldBeFalse();
    }

    [Fact]
    public async Task Disconnected_mqtt5_reports_protocol_only_features_as_supported()
    {
        await using var client = new ResilientMqttClient(
            new SequencedTransportFactory(),
            NewOptions(MqttProtocolVersion.V500));

        client.GetProtocolFeatureSupport(MqttProtocolFeature.RequestResponse)
            .ShouldBe(MqttBrokerFeatureSupport.Supported);
        client.GetProtocolFeatureSupport(MqttProtocolFeature.TraceContextUserProperties)
            .ShouldBe(MqttBrokerFeatureSupport.Supported);
        client.CanUseProtocolFeature(MqttProtocolFeature.RequestResponse).ShouldBeTrue();
    }

    [Fact]
    public async Task Disconnected_mqtt5_reports_broker_negotiated_features_as_unknown()
    {
        await using var client = new ResilientMqttClient(
            new SequencedTransportFactory(),
            NewOptions(MqttProtocolVersion.V500));

        client.GetProtocolFeatureSupport(MqttProtocolFeature.TopicAliases)
            .ShouldBe(MqttBrokerFeatureSupport.Unknown);
        client.GetProtocolFeatureSupport(MqttProtocolFeature.SubscriptionIdentifiers)
            .ShouldBe(MqttBrokerFeatureSupport.Unknown);
        client.GetProtocolFeatureSupport(MqttProtocolFeature.SharedSubscriptions)
            .ShouldBe(MqttBrokerFeatureSupport.Unknown);
    }

    [Fact]
    public async Task Connected_mqtt5_maps_broker_negotiated_features_from_capabilities()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions(MqttProtocolVersion.V500));

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(
            new MqttConnAckPacket
            {
                TopicAliasMaximum = 2,
                SubscriptionIdentifiersAvailable = false,
                SharedSubscriptionAvailable = true,
            },
            timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        client.GetProtocolFeatureSupport(MqttProtocolFeature.TopicAliases)
            .ShouldBe(MqttBrokerFeatureSupport.Supported);
        client.GetProtocolFeatureSupport(MqttProtocolFeature.SubscriptionIdentifiers)
            .ShouldBe(MqttBrokerFeatureSupport.NotSupported);
        client.GetProtocolFeatureSupport(MqttProtocolFeature.SharedSubscriptions)
            .ShouldBe(MqttBrokerFeatureSupport.Supported);
        client.GetBrokerCapabilitiesSnapshot()
            .ShouldNotBeNull()
            .GetFeatureSupport(MqttProtocolFeature.TopicAliases)
            .ShouldBe(MqttBrokerFeatureSupport.Supported);
    }

    [Fact]
    public async Task EnsureProtocolFeature_throws_on_not_supported_or_unknown_and_passes_on_supported()
    {
        await using var mqtt311Client = new ResilientMqttClient(
            new SequencedTransportFactory(),
            NewOptions(MqttProtocolVersion.V311));
        await using var mqtt5Client = new ResilientMqttClient(
            new SequencedTransportFactory(),
            NewOptions(MqttProtocolVersion.V500));

        Should.Throw<NotSupportedException>(() =>
                mqtt311Client.EnsureProtocolFeature(MqttProtocolFeature.RequestResponse, "RequestAsync"))
            .Message.ShouldContain("RequestAsync");

        Should.Throw<NotSupportedException>(() =>
                mqtt5Client.EnsureProtocolFeature(MqttProtocolFeature.TopicAliases, "topic alias publishing"))
            .Message.ShouldContain("unknown");

        Should.NotThrow(() =>
            mqtt5Client.EnsureProtocolFeature(MqttProtocolFeature.RequestResponse, "RequestAsync"));
    }

    private static ResilientMqttClientOptions NewOptions(MqttProtocolVersion protocolVersion) => new()
    {
        Connect = new MqttConnectPacket
        {
            ClientId = "protocol-feature-tests",
            ProtocolVersion = protocolVersion,
        },
        ReconnectStrategy = new BackoffReconnectStrategy(
            new BackoffOptions
            {
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(10),
            },
            new DefaultReconnectDecision(),
            new ZeroJitter()),
    };

    private static async Task WaitForStateAsync(
        ResilientMqttClient client,
        ConnectionState state,
        CancellationToken cancellationToken)
    {
        while (client.State != state)
        {
            await Task.Delay(1, cancellationToken);
        }
    }
}
