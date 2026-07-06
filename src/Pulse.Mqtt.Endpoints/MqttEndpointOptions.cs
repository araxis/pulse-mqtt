using Pulse.Mqtt.Client;

namespace Pulse.Mqtt.Endpoints;

/// <summary>Settings for one mapped endpoint: the subscription it creates and its local route.</summary>
public sealed record MqttEndpointOptions
{
    /// <summary>The maximum QoS requested from the broker. At-least-once by default.</summary>
    public MqttQualityOfService QualityOfService { get; init; } = MqttQualityOfService.AtLeastOnce;

    /// <summary>Whether the broker must not send messages this client published itself.</summary>
    public bool NoLocal { get; init; }

    /// <summary>Whether retained messages keep their retain flag when delivered.</summary>
    public bool RetainAsPublished { get; init; }

    /// <summary>How the broker replays retained messages for this subscription.</summary>
    public MqttRetainHandling RetainHandling { get; init; } = MqttRetainHandling.SendAtSubscribe;

    /// <summary>Whether this endpoint acknowledges after local routing or requires the handler to do it.</summary>
    public MqttAcknowledgementMode Acknowledgement { get; init; } = MqttAcknowledgementMode.Automatic;

    /// <summary>Local dispatch settings (queue bounds, concurrency) for the endpoint's route.</summary>
    public MqttRouteOptions? Route { get; init; }
}
