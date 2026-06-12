using Microsoft.Extensions.DependencyInjection;
using Pulse.Mqtt.Client;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>Resolves named Pulse MQTT clients, mirroring the named-client factory pattern.</summary>
public interface IPulseMqttClientFactory
{
    /// <summary>Returns the client registered under <paramref name="name"/>.</summary>
    ResilientMqttClient GetClient(string name);
}

internal sealed class PulseMqttClientFactory(IServiceProvider services) : IPulseMqttClientFactory
{
    public ResilientMqttClient GetClient(string name) =>
        services.GetRequiredKeyedService<ResilientMqttClient>(name);
}
