namespace Pulse.Mqtt.Client;

/// <summary>Describes whether a broker feature is known to be available on the current connection.</summary>
public enum MqttBrokerFeatureSupport
{
    /// <summary>The protocol or broker did not negotiate this feature, so support is unknown.</summary>
    Unknown = 0,

    /// <summary>The broker reported or implied that the feature is supported.</summary>
    Supported = 1,

    /// <summary>The broker reported or implied that the feature is not supported.</summary>
    NotSupported = 2,
}
