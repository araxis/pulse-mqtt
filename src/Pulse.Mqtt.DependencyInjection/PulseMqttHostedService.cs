using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>Starts a named client with the host and stops it on shutdown.</summary>
internal sealed class PulseMqttHostedService(ResilientMqttClient client) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => client.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => client.StopAsync(cancellationToken);
}
