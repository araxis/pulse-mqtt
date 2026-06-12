using Microsoft.Extensions.Diagnostics.HealthChecks;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Reports a client's connection state: connected is healthy, transitional states are degraded,
/// and faulted/stopped/disconnected are unhealthy.
/// </summary>
public sealed class PulseMqttHealthCheck(ResilientMqttClient client) : IHealthCheck
{
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var state = client.State;
        var result = state switch
        {
            ConnectionState.Connected => HealthCheckResult.Healthy($"Connected."),
            ConnectionState.Connecting or ConnectionState.Reconnecting or ConnectionState.WaitingRetry =>
                HealthCheckResult.Degraded($"The connection is being established ({state})."),
            _ => HealthCheckResult.Unhealthy($"The client is {state}."),
        };

        return Task.FromResult(result);
    }
}
