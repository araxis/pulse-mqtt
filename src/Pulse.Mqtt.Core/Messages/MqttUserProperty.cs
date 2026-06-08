namespace Pulse.Mqtt;

/// <summary>An MQTT 5 user property: a free-form name/value pair carried on a packet.</summary>
/// <param name="Name">The property name.</param>
/// <param name="Value">The property value.</param>
public sealed record MqttUserProperty(string Name, string Value);
