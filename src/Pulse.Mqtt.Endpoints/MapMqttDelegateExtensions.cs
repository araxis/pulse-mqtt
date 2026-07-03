using Microsoft.Extensions.Hosting;
using Pulse.Mqtt.Client;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Minimal-API-style handler signatures for <c>MapMqtt</c>: write the parameters you need —
/// route values (typed by their constraints), the payload, services, the context, the message,
/// a <see cref="CancellationToken"/> — in any order, and the Pulse.Mqtt.Endpoints source
/// generator lowers the call onto the explicit context API at compile time. There is no runtime
/// binder and no reflection: a call site the generator cannot bind is a compile error, and these
/// fallback bodies only run when the generator did not process the compilation at all.
/// </summary>
public static class MapMqttDelegateExtensions
{
    /// <summary>Maps a Minimal-API-style handler on the client. Lowered at compile time.</summary>
    public static MqttEndpoint MapMqtt(
        this ResilientMqttClient client,
        string template,
        Delegate handler,
        MqttEndpointOptions? options = null,
        IServiceProvider? services = null) =>
        throw GeneratorMissing();

    /// <summary>Maps a Minimal-API-style handler on the app's single client. Lowered at compile time.</summary>
    public static MqttEndpoint MapMqtt(
        this IHost app,
        string template,
        Delegate handler,
        MqttEndpointOptions? options = null) =>
        throw GeneratorMissing();

    /// <summary>Maps a Minimal-API-style handler on the named client. Lowered at compile time.</summary>
    public static MqttEndpoint MapMqtt(
        this IHost app,
        string clientName,
        string template,
        Delegate handler,
        MqttEndpointOptions? options = null) =>
        throw GeneratorMissing();

    /// <summary>
    /// Maps a Minimal-API-style request/reply handler on the client: the return value is the
    /// reply. Lowered at compile time.
    /// </summary>
    public static MqttEndpoint MapMqttRequest(
        this ResilientMqttClient client,
        string template,
        Delegate handler,
        MqttEndpointOptions? options = null,
        IServiceProvider? services = null) =>
        throw GeneratorMissing();

    /// <summary>
    /// Maps a Minimal-API-style request/reply handler on the app's single client: the return
    /// value is the reply. Lowered at compile time.
    /// </summary>
    public static MqttEndpoint MapMqttRequest(
        this IHost app,
        string template,
        Delegate handler,
        MqttEndpointOptions? options = null) =>
        throw GeneratorMissing();

    /// <summary>
    /// Maps a Minimal-API-style request/reply handler on the named client: the return value is
    /// the reply. Lowered at compile time.
    /// </summary>
    public static MqttEndpoint MapMqttRequest(
        this IHost app,
        string clientName,
        string template,
        Delegate handler,
        MqttEndpointOptions? options = null) =>
        throw GeneratorMissing();

    private static InvalidOperationException GeneratorMissing() =>
        new("This MapMqtt overload is lowered at compile time by the Pulse.Mqtt.Endpoints source " +
            "generator, which did not run. Build with the Pulse.Mqtt.Endpoints NuGet package (it " +
            "ships the generator), or use the MqttEndpointContext overloads directly.");
}
