namespace Pulse.Mqtt.Client;

/// <summary>Settings for a server-streamed request/response call.</summary>
public sealed record MqttRequestStreamOptions
{
    /// <summary>
    /// How long to wait for the next response before failing with <see cref="MqttException"/>. The
    /// clock resets on every response, so this bounds the gap between items, not the total duration.
    /// </summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The QoS used for the request publish.</summary>
    public MqttQualityOfService QualityOfService { get; init; } = MqttQualityOfService.AtLeastOnce;

    /// <summary>
    /// The number of responses buffered before backpressure throttles delivery. A slow consumer
    /// fills the buffer and then slows the producer rather than buffering without limit.
    /// </summary>
    public int Capacity { get; init; } = 64;
}
