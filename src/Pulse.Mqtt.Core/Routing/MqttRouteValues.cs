namespace Pulse.Mqtt.Routing;

/// <summary>The parameter values captured when a topic matched a route template.</summary>
public readonly struct MqttRouteValues
{
    private readonly string[]? _names;
    private readonly string[]? _values;

    internal MqttRouteValues(string[] names, string[] values)
    {
        _names = names;
        _values = values;
    }

    /// <summary>No captured parameters.</summary>
    public static MqttRouteValues Empty => default;

    /// <summary>The number of captured parameters.</summary>
    public int Count => _names?.Length ?? 0;

    /// <summary>Returns the value captured for <paramref name="name"/>.</summary>
    /// <exception cref="KeyNotFoundException">The template has no parameter with that name.</exception>
    public string this[string name] =>
        TryGetValue(name, out var value) ? value : throw new KeyNotFoundException($"No route parameter named '{name}'.");

    /// <summary>Attempts to return the value captured for <paramref name="name"/>.</summary>
    public bool TryGetValue(string name, out string value)
    {
        if (_names is { } names && _values is { } values)
        {
            for (var i = 0; i < names.Length; i++)
            {
                if (string.Equals(names[i], name, StringComparison.Ordinal))
                {
                    value = values[i];
                    return true;
                }
            }
        }

        value = string.Empty;
        return false;
    }
}
