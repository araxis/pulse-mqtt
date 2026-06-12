namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent entry points on <see cref="ResilientMqttClient"/>. Each returns a small builder that
/// composes one operation and runs it through the client's regular APIs — same semantics, same
/// guarantees, just chainable.
/// </summary>
public static class ResilientMqttClientFluentExtensions
{
    /// <summary>Starts composing a publish to <paramref name="topic"/>.</summary>
    public static MqttPublishBuilder Publish(this ResilientMqttClient client, string topic)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return new MqttPublishBuilder(client, topic);
    }

    /// <summary>Starts composing a route for <paramref name="template"/> (for example <c>sensors/{deviceId}/temp</c>).</summary>
    public static MqttRouteBuilder Route(this ResilientMqttClient client, string template)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(template);
        return new MqttRouteBuilder(client, template);
    }

    /// <summary>Starts composing a request to <paramref name="topic"/>.</summary>
    public static MqttRequestBuilder Request(this ResilientMqttClient client, string topic)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return new MqttRequestBuilder(client, topic);
    }
}
