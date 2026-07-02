using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Pulse.Mqtt.Transport;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests.Brokers;

/// <summary>One MQTT broker under test: its host, mapped port, and a name for diagnostics.</summary>
public interface IMqttBroker
{
    string Name { get; }

    string Host { get; }

    int Port { get; }

    /// <summary>How the scenarios reach this broker; plain TCP unless a view overrides it.</summary>
    IMqttTransportFactory CreateTransport() =>
        new TcpTransportFactory(new TcpTransportOptions { Host = Host, Port = Port });
}

/// <summary>Base fixture: owns a container for the whole collection and maps the MQTT port.</summary>
public abstract class BrokerFixture : IMqttBroker, IAsyncLifetime
{
    private IContainer? _container;

    public abstract string Name { get; }

    public string Host => Container.Hostname;

    public int Port => Container.GetMappedPublicPort(1883);

    private IContainer Container => _container ?? throw new InvalidOperationException("The broker container is not started.");

    /// <summary>Resolves a mapped port by its container binding, such as <c>"14567/udp"</c>.</summary>
    protected int GetMappedPublicPort(string containerPort) => Container.GetMappedPublicPort(containerPort);

    public Task InitializeAsync()
    {
        _container = Build().Build();
        return _container.StartAsync();
    }

    public Task DisposeAsync() => _container?.DisposeAsync().AsTask() ?? Task.CompletedTask;

    protected abstract ContainerBuilder Build();
}

// Mosquitto already has a dedicated fixture and "mosquitto" collection (see MosquittoFixture),
// reused here so the existing integration tests and the matrix share one container definition.

/// <summary>EMQX 5; anonymous MQTT access is the out-of-the-box behavior.</summary>
public sealed class EmqxBroker : BrokerFixture
{
    public override string Name => "EMQX 5.8";

    /// <summary>The mapped UDP port of the MQTT-over-QUIC listener.</summary>
    public int QuicPort => GetMappedPublicPort("14567/udp");

    // The QUIC listener is off by default and uses the self-signed certificates the image ships.
    protected override ContainerBuilder Build() => new ContainerBuilder("emqx/emqx:5.8")
        .WithPortBinding(1883, assignRandomHostPort: true)
        .WithPortBinding("14567/udp", assignRandomHostPort: true)
        .WithEnvironment("EMQX_LISTENERS__QUIC__DEFAULT__ENABLED", "true")
        .WithEnvironment("EMQX_LISTENERS__QUIC__DEFAULT__BIND", "0.0.0.0:14567")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(1883));
}

/// <summary>The EMQX fixture reached through its QUIC listener instead of TCP.</summary>
public sealed class EmqxQuicBroker(EmqxBroker inner) : IMqttBroker
{
    public string Name => $"{inner.Name} over QUIC";

    public string Host => inner.Host;

    public int Port => inner.QuicPort;

    public IMqttTransportFactory CreateTransport() => new QuicTransportFactory(new QuicTransportOptions
    {
        Host = Host,
        Port = Port,
        // The listener uses the image's self-signed demo certificate.
        ServerCertificateValidation = (_, _, _, _) => true,
    });
}

/// <summary>HiveMQ Community Edition; anonymous access by default.</summary>
public sealed class HiveMqBroker : BrokerFixture
{
    public override string Name => "HiveMQ CE 2024.3";

    protected override ContainerBuilder Build() => new ContainerBuilder("hivemq/hivemq-ce:2024.3")
        .WithPortBinding(1883, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Started HiveMQ"));
}

[CollectionDefinition("emqx")]
public sealed class EmqxCollection : ICollectionFixture<EmqxBroker>;

[CollectionDefinition("hivemq")]
public sealed class HiveMqCollection : ICollectionFixture<HiveMqBroker>;
