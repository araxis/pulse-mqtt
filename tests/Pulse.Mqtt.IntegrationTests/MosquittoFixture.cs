using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

/// <summary>
/// Owns one Mosquitto broker container for the whole test collection: image, anonymous-access
/// configuration, random host port, readiness wait, and cleanup.
/// </summary>
public sealed class MosquittoFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder("eclipse-mosquitto:2")
        .WithCommand("mosquitto", "-c", "/mosquitto-no-auth.conf")
        .WithPortBinding(1883, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(1883))
        .Build();

    public string Host => _container.Hostname;

    public int Port => _container.GetMappedPublicPort(1883);

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition("mosquitto")]
public sealed class MosquittoCollection : ICollectionFixture<MosquittoFixture>;
