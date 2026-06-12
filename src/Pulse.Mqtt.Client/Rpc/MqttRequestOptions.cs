namespace Pulse.Mqtt.Client;

/// <summary>Settings for a request/response call.</summary>
public sealed record MqttRequestOptions
{
    /// <summary>How long to wait for the response before failing with <see cref="MqttException"/>.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The QoS used for the request publish.</summary>
    public MqttQualityOfService QualityOfService { get; init; } = MqttQualityOfService.AtLeastOnce;
}
