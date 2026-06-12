using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Ties a named client to the host: starts it on startup unless the client is configured for
/// manual control, and stops it on shutdown either way. Stopping is idempotent, so a client the
/// application already stopped — or never started — shuts down cleanly.
/// </summary>
internal sealed class PulseMqttHostedService(ResilientMqttClient client, bool startWithHost) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) =>
        startWithHost ? client.StartAsync(cancellationToken) : Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => client.StopAsync(cancellationToken);
}
