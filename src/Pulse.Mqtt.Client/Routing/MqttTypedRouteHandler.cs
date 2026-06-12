namespace Pulse.Mqtt.Client;

/// <summary>Handles one routed message whose payload was deserialized to <typeparamref name="T"/>.</summary>
/// <param name="value">The deserialized payload.</param>
/// <param name="message">The raw routed message, including captured route parameters.</param>
/// <param name="cancellationToken">Signals shutdown.</param>
public delegate ValueTask MqttTypedRouteHandler<in T>(T value, MqttRoutedMessage message, CancellationToken cancellationToken);
