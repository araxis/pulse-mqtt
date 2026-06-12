using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Serialization;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// Configures one named client. Every <c>Use*</c> method swaps a single behavior; anything not
/// swapped uses the built-in default. Registrations are keyed by the client name, so different
/// clients can use different implementations side by side.
/// </summary>
public sealed class PulseMqttBuilder
{
    internal PulseMqttBuilder(IServiceCollection services, string name)
    {
        Services = services;
        Name = name;
    }

    /// <summary>The service collection being configured.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The client name this builder configures.</summary>
    public string Name { get; }

    /// <summary>Replaces the transport (default: TCP/TLS from the options' host and port).</summary>
    public PulseMqttBuilder UseTransportFactory(Func<IServiceProvider, IMqttTransportFactory> factory) =>
        AddKeyed(factory);

    /// <summary>Replaces the reconnect loop (default: exponential backoff with jitter).</summary>
    public PulseMqttBuilder UseReconnectStrategy(Func<IServiceProvider, IReconnectStrategy> factory) =>
        AddKeyed(factory);

    /// <summary>Replaces the retry-or-fault classification of connect failures.</summary>
    public PulseMqttBuilder UseReconnectDecision(Func<IServiceProvider, IReconnectDecision> factory) =>
        AddKeyed(factory);

    /// <summary>Replaces the connection up/down hook (default: re-subscribe from the session store).</summary>
    public PulseMqttBuilder UseLifecycle(Func<IServiceProvider, IConnectionLifecycle> factory) =>
        AddKeyed(factory);

    /// <summary>Replaces the session store (default: in-memory).</summary>
    public PulseMqttBuilder UseSessionStore(Func<IServiceProvider, ISessionStore> factory) =>
        AddKeyed(factory);

    /// <summary>Replaces the offline message store (default: bounded in-memory).</summary>
    public PulseMqttBuilder UseMessageStore(Func<IServiceProvider, IMessageStore> factory) =>
        AddKeyed(factory);

    /// <summary>Sets the payload serializer for typed messaging (no default).</summary>
    public PulseMqttBuilder UseSerializer(Func<IServiceProvider, IMqttSerializer> factory) =>
        AddKeyed(factory);

    /// <summary>Registers a health check named <c>pulse-mqtt-&lt;name&gt;</c> for this client.</summary>
    public PulseMqttBuilder AddHealthCheck()
    {
        var clientName = Name;
        Services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"pulse-mqtt-{clientName}",
            provider => new PulseMqttHealthCheck(provider.GetRequiredKeyedService<ResilientMqttClient>(clientName)),
            failureStatus: null,
            tags: null));
        return this;
    }

    private PulseMqttBuilder AddKeyed<TService>(Func<IServiceProvider, TService> factory)
        where TService : class
    {
        ArgumentNullException.ThrowIfNull(factory);
        Services.AddKeyedSingleton<TService>(Name, (provider, _) => factory(provider));
        return this;
    }
}
