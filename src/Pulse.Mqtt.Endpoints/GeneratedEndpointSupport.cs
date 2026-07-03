using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Helpers the Pulse.Mqtt.Endpoints source generator emits calls to. Not intended for direct
/// application use — the <c>MapMqtt</c> extensions are the public surface.
/// </summary>
public static class GeneratedEndpointSupport
{
    /// <summary>Resolves the app's single registered client, with actionable errors otherwise.</summary>
    public static ResilientMqttClient ResolveSingleClient(IHost app)
    {
        ArgumentNullException.ThrowIfNull(app);
        var names = app.Services.GetServices<PulseMqttClientRegistration>()
            .Select(registration => registration.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        return names switch
        {
            [] => throw new InvalidOperationException(
                "No Pulse MQTT client is registered. Call services.AddPulseMqttClient(\"name\", ...) first."),
            [var single] => ResolveClient(app, single),
            _ => throw new InvalidOperationException(
                $"Several Pulse MQTT clients are registered ({string.Join(", ", names.Select(n => $"'{n}'"))}); " +
                "use the MapMqtt overload that names the client."),
        };
    }

    /// <summary>Resolves the named registered client.</summary>
    public static ResilientMqttClient ResolveClient(IHost app, string clientName)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.Services.GetRequiredService<IPulseMqttClientFactory>().GetClient(clientName);
    }
}
