using System.Globalization;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Typed access to the route parameters an endpoint's template captured. The typed getters parse
/// with the invariant culture; pair them with the matching template constraint
/// (<c>{id:int}</c>, <c>{id:long}</c>, <c>{id:guid}</c>, <c>{flag:bool}</c>) so a non-conforming
/// topic never reaches the handler in the first place.
/// </summary>
public readonly struct MqttEndpointRouteValues
{
    private readonly MqttRouteValues _values;

    internal MqttEndpointRouteValues(MqttRouteValues values)
    {
        _values = values;
    }

    /// <summary>The raw captured values.</summary>
    public MqttRouteValues Raw => _values;

    /// <summary>Returns the value captured for <paramref name="name"/>.</summary>
    /// <exception cref="KeyNotFoundException">The template has no parameter with that name.</exception>
    public string GetString(string name) => _values[name];

    /// <summary>Returns the value captured for <paramref name="name"/> as an <see cref="int"/>.</summary>
    public int GetInt(string name) =>
        int.TryParse(_values[name], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw NotA(name, "an int", "{" + name + ":int}");

    /// <summary>Returns the value captured for <paramref name="name"/> as a <see cref="long"/>.</summary>
    public long GetLong(string name) =>
        long.TryParse(_values[name], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw NotA(name, "a long", "{" + name + ":long}");

    /// <summary>Returns the value captured for <paramref name="name"/> as a <see cref="System.Guid"/>.</summary>
    public Guid GetGuid(string name) =>
        Guid.TryParse(_values[name], out var value)
            ? value
            : throw NotA(name, "a guid", "{" + name + ":guid}");

    /// <summary>Returns the value captured for <paramref name="name"/> as a <see cref="bool"/>.</summary>
    public bool GetBool(string name) =>
        bool.TryParse(_values[name], out var value)
            ? value
            : throw NotA(name, "a bool", "{" + name + ":bool}");

    private InvalidOperationException NotA(string name, string kind, string constraint) =>
        new($"Route parameter '{name}' captured '{_values[name]}', which is not {kind}. " +
            $"Constrain the template with {constraint} so non-conforming topics never match.");
}
