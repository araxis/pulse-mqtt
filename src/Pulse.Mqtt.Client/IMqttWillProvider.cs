using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Computes the MQTT last-will message for each connection attempt.
/// </summary>
/// <remarks>
/// A provider is useful when the will depends on connection context, current service state, or
/// other dependencies. Return <see langword="null"/> to connect without a will for that attempt.
/// Throwing fails that connection attempt and lets the configured reconnect policy decide whether
/// to retry.
/// </remarks>
public interface IMqttWillProvider
{
    /// <summary>Creates the will for the current connection attempt.</summary>
    ValueTask<MqttWillMessage?> CreateWillAsync(MqttWillContext context, CancellationToken cancellationToken);
}
