using Microsoft.Extensions.Hosting;

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
        GeneratedEndpointSupport.ResolveSingleClient(app).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps a typed endpoint on the app's single registered Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt<TPayload>(
        this IHost app,
        string template,
        Func<TPayload, MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        GeneratedEndpointSupport.ResolveSingleClient(app).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps an endpoint on the named Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt(
        this IHost app,
        string clientName,
        string template,
        Func<MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        GeneratedEndpointSupport.ResolveClient(app, clientName).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps a typed endpoint on the named Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqtt<TPayload>(
        this IHost app,
        string clientName,
        string template,
        Func<TPayload, MqttEndpointContext, ValueTask> handler,
        MqttEndpointOptions? options = null) =>
        GeneratedEndpointSupport.ResolveClient(app, clientName).MapMqtt(template, handler, options, app.Services);

    /// <summary>Maps a request/reply endpoint on the app's single registered Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqttRequest<TRequest, TResponse>(
        this IHost app,
        string template,
        Func<TRequest, MqttEndpointContext, ValueTask<TResponse>> handler,
        MqttEndpointOptions? options = null) =>
        GeneratedEndpointSupport.ResolveSingleClient(app).MapMqttRequest(template, handler, options, app.Services);

    /// <summary>Maps a request/reply endpoint on the named Pulse MQTT client.</summary>
    public static MqttEndpoint MapMqttRequest<TRequest, TResponse>(
        this IHost app,
        string clientName,
        string template,
        Func<TRequest, MqttEndpointContext, ValueTask<TResponse>> handler,
        MqttEndpointOptions? options = null) =>
        GeneratedEndpointSupport.ResolveClient(app, clientName).MapMqttRequest(template, handler, options, app.Services);
}
