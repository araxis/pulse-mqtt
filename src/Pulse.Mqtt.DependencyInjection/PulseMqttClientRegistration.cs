namespace Pulse.Mqtt.DependencyInjection;

/// <summary>
/// A marker recorded once per <c>AddPulseMqttClient</c> call, so consumers such as the endpoints
/// package can discover the registered client names — for example to resolve "the app's single
/// client" without naming it.
/// </summary>
/// <param name="Name">The client name passed to <c>AddPulseMqttClient</c>.</param>
public sealed record PulseMqttClientRegistration(string Name);
