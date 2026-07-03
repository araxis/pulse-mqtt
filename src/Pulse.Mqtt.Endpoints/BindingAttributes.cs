namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Binds a handler parameter to a route template parameter, overriding name-based matching.
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class FromRouteAttribute : Attribute
{
    /// <summary>Binds to the route parameter with this name; the parameter's own name by default.</summary>
    public string? Name { get; init; }
}

/// <summary>Binds a handler parameter to the deserialized message payload.</summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class FromPayloadAttribute : Attribute;

/// <summary>Binds a handler parameter to a service resolved from the per-message scope.</summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class FromServicesAttribute : Attribute;
