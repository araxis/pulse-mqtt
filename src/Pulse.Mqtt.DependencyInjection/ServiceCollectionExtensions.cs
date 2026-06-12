using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Serialization;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.DependencyInjection;

/// <summary>Registers Pulse MQTT clients with the service collection.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a named, host-managed <see cref="ResilientMqttClient"/>. Resolve it via
    /// <see cref="IPulseMqttClientFactory"/> or as a keyed service; it starts and stops with the
    /// host. Call once per name; swap behaviors through the returned builder.
    /// </summary>
    public static PulseMqttBuilder AddPulseMqttClient(
        this IServiceCollection services,
        string name,
        Action<PulseMqttClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<PulseMqttClientOptions>(name).Configure(configure);
        services.TryAddSingleton<IPulseMqttClientFactory, PulseMqttClientFactory>();

        services.AddKeyedSingleton(name, (provider, key) =>
        {
            var clientName = (string)key!;
            var options = provider.GetRequiredService<IOptionsMonitor<PulseMqttClientOptions>>().Get(clientName);
            Validate(clientName, options);

            var transportFactory = provider.GetKeyedService<IMqttTransportFactory>(clientName)
                ?? new TcpTransportFactory(new TcpTransportOptions
                {
                    Host = options.Host,
                    Port = options.Port,
                    UseTls = options.UseTls,
                });

            var connect = new MqttConnectPacket
            {
                ClientId = options.ClientId,
                ProtocolVersion = options.ProtocolVersion,
                CleanStart = options.CleanStart,
                KeepAliveSeconds = options.KeepAliveSeconds,
                Username = options.Username,
                Password = options.Password is { } password ? Encoding.UTF8.GetBytes(password) : null,
            };

            var clientOptions = new ResilientMqttClientOptions
            {
                Connect = connect,
                ReconnectStrategy = provider.GetKeyedService<IReconnectStrategy>(clientName),
                ReconnectDecision = provider.GetKeyedService<IReconnectDecision>(clientName),
                Lifecycle = provider.GetKeyedService<IConnectionLifecycle>(clientName),
                SessionStore = provider.GetKeyedService<ISessionStore>(clientName),
                MessageStore = provider.GetKeyedService<IMessageStore>(clientName),
                Serializer = provider.GetKeyedService<IMqttSerializer>(clientName),
            };

            return new ResilientMqttClient(transportFactory, clientOptions, provider.GetService<TimeProvider>());
        });

        services.AddSingleton<IHostedService>(provider =>
            new PulseMqttHostedService(provider.GetRequiredKeyedService<ResilientMqttClient>(name)));

        return new PulseMqttBuilder(services, name);
    }

    private static void Validate(string name, PulseMqttClientOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Host))
        {
            throw new InvalidOperationException($"Client '{name}': configure a broker host.");
        }

        if (options.Port is < 1 or > 65535)
        {
            throw new InvalidOperationException($"Client '{name}': port {options.Port} is out of range.");
        }

        if (string.IsNullOrWhiteSpace(options.ClientId))
        {
            throw new InvalidOperationException($"Client '{name}': configure a client identifier.");
        }
    }
}
