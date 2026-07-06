namespace Pulse.Mqtt.Client;

/// <summary>How a routed inbound message sends its MQTT protocol acknowledgement.</summary>
public enum MqttAcknowledgementMode
{
    /// <summary>The client acknowledges after it accepts the message into local routing.</summary>
    Automatic,

    /// <summary>The route handler or stream consumer must explicitly acknowledge or reject the message.</summary>
    Manual,
}
