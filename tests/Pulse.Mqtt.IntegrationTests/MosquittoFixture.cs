using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Pulse.Mqtt.IntegrationTests.Brokers;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

/// <summary>
/// Owns one Mosquitto broker container for the whole test collection: image, anonymous-access
/// configuration, random host port, readiness wait, and cleanup.
/// </summary>
public sealed class MosquittoFixture : IMqttBroker, IAsyncLifetime
{
    private readonly (string Host, int Port)? _external = ParseExternalBroker();
    private readonly IContainer? _container;

    public MosquittoFixture()
    {
        // Default: a throwaway Docker container. When PULSE_MQTT_BROKER points at an already-running
        // broker, skip Docker entirely (useful when Docker is unavailable — run a native mosquitto.exe).
        if (_external is null)
        {
            _container = new ContainerBuilder("eclipse-mosquitto:2")
                .WithCommand("mosquitto", "-c", "/mosquitto-no-auth.conf")
                .WithPortBinding(1883, assignRandomHostPort: true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(1883))
                .Build();
        }
    }

    public string Name => "Mosquitto 2";

    public string Host => _external?.Host ?? _container!.Hostname;

    public int Port => _external?.Port ?? _container!.GetMappedPublicPort(1883);

    public Task InitializeAsync() => _container?.StartAsync() ?? Task.CompletedTask;

    public Task DisposeAsync() => _container?.DisposeAsync().AsTask() ?? Task.CompletedTask;

    // "host:port" of an already-running broker to use instead of a Docker container; null when unset.
    private static (string Host, int Port)? ParseExternalBroker()
    {
        var value = Environment.GetEnvironmentVariable("PULSE_MQTT_BROKER");
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var parts = value.Split(':', 2);
        return (parts[0], parts.Length > 1 && int.TryParse(parts[1], out var port) ? port : 1883);
    }
}

[CollectionDefinition("mosquitto")]
public sealed class MosquittoCollection : ICollectionFixture<MosquittoFixture>;
