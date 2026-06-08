namespace Pulse.Mqtt;

/// <summary>Controls whether retained messages are sent when a subscription is established (MQTT 5).</summary>
public enum MqttRetainHandling : byte
{
    /// <summary>Send retained messages at the time of subscribe.</summary>
    SendAtSubscribe = 0,

    /// <summary>Send retained messages at subscribe only if the subscription does not currently exist.</summary>
    SendAtSubscribeIfNewSubscription = 1,

    /// <summary>Do not send retained messages at the time of subscribe.</summary>
    DoNotSendAtSubscribe = 2,
}
