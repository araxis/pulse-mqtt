using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent composition of one request/response call. Created by
/// <see cref="ResilientMqttClientFluentExtensions.Request"/>; finished with a
/// <see cref="SendAsync{TRequest, TResponse}"/> (typed) or <see cref="SendAsync"/> (raw) terminal.
/// </summary>
/// <example>
/// <code>
/// var reply = await client.Request("devices/boiler-1/status")
///     .WithTimeout(TimeSpan.FromSeconds(5))
///     .SendAsync&lt;StatusRequest, StatusReply&gt;(new StatusRequest("dashboard"), ct);
/// </code>
/// </example>
public sealed class MqttRequestBuilder
{
    private readonly ResilientMqttClient _client;
    private readonly string _topic;
    private MqttRequestOptions _options = new();
    private ReadOnlyMemory<byte> _payload;
    private string? _contentType;

    internal MqttRequestBuilder(ResilientMqttClient client, string topic)
    {
        _client = client;
        _topic = topic;
    }

    /// <summary>How long to wait for the reply before failing with <see cref="MqttException"/>.</summary>
    public MqttRequestBuilder WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The timeout must be positive.");
        }

        _options = _options with { Timeout = timeout };
        return this;
    }

    /// <summary>The QoS used for the request publish.</summary>
    public MqttRequestBuilder WithQualityOfService(MqttQualityOfService qualityOfService)
    {
        _options = _options with { QualityOfService = qualityOfService };
        return this;
    }

    /// <summary>Sets a raw request payload, for the raw <see cref="SendAsync"/> terminal.</summary>
    public MqttRequestBuilder WithPayload(ReadOnlyMemory<byte> payload)
    {
        _payload = payload;
        return this;
    }

    /// <summary>Sets the MQTT 5 content type on the raw request.</summary>
    public MqttRequestBuilder WithContentType(string contentType)
    {
        ArgumentException.ThrowIfNullOrEmpty(contentType);
        _contentType = contentType;
        return this;
    }

    /// <summary>Serializes <paramref name="request"/>, sends it, and deserializes the reply.</summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    /// <exception cref="MqttException">No reply arrived within the timeout.</exception>
    public Task<TResponse> SendAsync<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken = default) =>
        _client.RequestAsync<TRequest, TResponse>(_topic, request, _options, cancellationToken);

    /// <summary>Sends the composed raw request and returns the raw reply.</summary>
    /// <exception cref="MqttException">No reply arrived within the timeout.</exception>
    public Task<MqttPublishPacket> SendAsync(CancellationToken cancellationToken = default) =>
        _client.RequestAsync(
            new MqttPublishPacket
            {
                Topic = _topic,
                Payload = _payload,
                ContentType = _contentType,
            },
            _options,
            cancellationToken);
}
