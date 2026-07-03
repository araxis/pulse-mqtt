using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// <c>app.MapMqtt(...)</c> — thin helpers over <see cref="PulseMqttEndpointExtensions"/> that
/// resolve the registered client and bind the host's services, so every invocation gets a
/// per-message scope. With one registered client the name can be omitted; with several, use the
/// overloads that take the client name.
/// </summary>
public static class HostMqttEndpointExtensions
{
    /// <summary>Maps an endpoint on the app's single registered Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt(
        this IHost app,
        string template,
        Func<MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        Client(app, SingleClientName(app)).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps a typed endpoint on the app's single registered Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt<TPayload>(
        this IHost app,
        string template,
        Func<TPayload, MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        Client(app, SingleClientName(app)).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps an endpoint on the named Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt(
        this IHost app,
        string clientName,
        string template,
        Func<MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        Client(app, clientName).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps a typed endpoint on the named Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt<TPayload>(
        this IHost app,
        string clientName,
        string template,
        Func<TPayload, MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        Client(app, clientName).MapMqtt(template, handler, options, app.Services);

    private static ResilientMqttClient Client(IHost app, string name)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.Services.GetRequiredService<IPulseMqttClientFactory>().GetClient(name);
    }

    private static string SingleClientName(IHost app)
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
            [var single] => single,
            _ => throw new InvalidOperationException(
                $"Several Pulse MQTT clients are registered ({string.Join(", ", names.Select(n => $"'{n}'"))}); " +
                "use the MapMqtt overload that names the client."),
        };
    }
}
