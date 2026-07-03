namespace Pulse.Mqtt.Routing;

/// <summary>
/// The value constraint of a route parameter, written as <c>{name:constraint}</c> in a route
/// template. A topic level that does not satisfy the constraint does not match the route at all,
/// mirroring ASP.NET Core route constraint semantics. The set is closed and every check is a
/// culture-invariant <c>TryParse</c>, so matching stays reflection-free and Native-AOT-safe.
/// </summary>
public enum MqttRouteConstraint
{
    /// <summary>No constraint: any single topic level matches.</summary>
    None,

    /// <summary>The level must parse as an invariant <see cref="int"/> (<c>{id:int}</c>).</summary>
    Int,

    /// <summary>The level must parse as an invariant <see cref="long"/> (<c>{id:long}</c>).</summary>
    Long,

    /// <summary>The level must parse as a <see cref="System.Guid"/> (<c>{id:guid}</c>).</summary>
    Guid,

    /// <summary>The level must parse as a <see cref="bool"/> (<c>{flag:bool}</c>).</summary>
    Bool,
}
