using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests.Brokers;

/// <summary>One MQTT broker under test: its host, mapped port, and a name for diagnostics.</summary>
public interface IMqttBroker
{
    string Name { get; }

    string Host { get; }

    int Port { get; }
}

/// <summary>Base fixture: owns a container for the whole collection and maps the MQTT port.</summary>
public abstract class BrokerFixture : IMqttBroker, IAsyncLifetime
{
    private IContainer? _container;

    public abstract string Name { get; }

    public string Host => Container.Hostname;

    public int Port => Container.GetMappedPublicPort(1883);

    private IContainer Container => _container ?? throw new InvalidOperationException("The broker container is not started.");

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

    protected override ContainerBuilder Build() => new ContainerBuilder("emqx/emqx:5.8")
        .WithPortBinding(1883, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(1883));
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
