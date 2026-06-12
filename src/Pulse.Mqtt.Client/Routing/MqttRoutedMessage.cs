using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>A received message together with the route parameters captured from its topic.</summary>
/// <param name="Message">The received PUBLISH.</param>
/// <param name="Values">The parameter values the route template captured.</param>
public readonly record struct MqttRoutedMessage(MqttPublishPacket Message, MqttRouteValues Values);
