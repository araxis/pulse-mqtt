using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Ties a named client to the host: connects it on startup unless the client is configured for
/// manual control, and disconnects it on shutdown either way. Disconnecting is idempotent, so a
/// client the application already disconnected — or never connected — shuts down cleanly.
/// </summary>
internal sealed class PulseMqttHostedService(ResilientMqttClient client, bool connectWithHost) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) =>
        connectWithHost ? client.ConnectAsync(cancellationToken) : Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => client.DisconnectAsync(cancellationToken);
}
