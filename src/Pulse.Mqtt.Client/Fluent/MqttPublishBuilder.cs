using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent composition of one publish. Created by
/// <see cref="ResilientMqttClientFluentExtensions.Publish"/>; finished with <see cref="SendAsync"/>.
/// </summary>
/// <example>
/// <code>
/// var outcome = await client.Publish("sensors/boiler-1/telemetry")
///     .AtLeastOnce()
///     .WithRetain()
///     .WithUserProperty("tenant", "acme")
///     .WithPayload(reading)
///     .SendAsync(ct);
/// </code>
/// </example>
public sealed class MqttPublishBuilder
{
    private readonly ResilientMqttClient _client;
    private readonly string _topic;
    private MqttQualityOfService _qualityOfService = MqttQualityOfService.AtMostOnce;
    private bool _retain;
    private ReadOnlyMemory<byte> _payload;
    private string? _contentType;
    private MqttPayloadFormatIndicator _payloadFormat = MqttPayloadFormatIndicator.Unspecified;
    private uint? _messageExpiryInterval;
    private string? _responseTopic;
    private ReadOnlyMemory<byte>? _correlationData;
    private List<MqttUserProperty>? _userProperties;

    internal MqttPublishBuilder(ResilientMqttClient client, string topic)
    {
        _client = client;
        _topic = topic;
    }

    /// <summary>Publishes at QoS 0 — the default.</summary>
    public MqttPublishBuilder AtMostOnce() => WithQualityOfService(MqttQualityOfService.AtMostOnce);

    /// <summary>Publishes at QoS 1: awaited to the broker's PUBACK.</summary>
    public MqttPublishBuilder AtLeastOnce() => WithQualityOfService(MqttQualityOfService.AtLeastOnce);

    /// <summary>Publishes at QoS 2: awaited through the full PUBREC/PUBREL/PUBCOMP exchange.</summary>
    public MqttPublishBuilder ExactlyOnce() => WithQualityOfService(MqttQualityOfService.ExactlyOnce);

    /// <summary>Sets the delivery quality of service.</summary>
    public MqttPublishBuilder WithQualityOfService(MqttQualityOfService qualityOfService)
    {
        _qualityOfService = qualityOfService;
        return this;
    }

    /// <summary>Asks the broker to retain the message for the topic.</summary>
    public MqttPublishBuilder WithRetain(bool retain = true)
    {
        _retain = retain;
        return this;
    }

    /// <summary>Sets a raw payload.</summary>
    public MqttPublishBuilder WithPayload(ReadOnlyMemory<byte> payload)
    {
        _payload = payload;
        return this;
    }

    /// <summary>Sets a UTF-8 text payload and marks the payload format accordingly.</summary>
    public MqttPublishBuilder WithPayload(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        _payload = System.Text.Encoding.UTF8.GetBytes(text);
        _payloadFormat = MqttPayloadFormatIndicator.Utf8;
        return this;
    }

    /// <summary>
    /// Serializes <paramref name="value"/> through the client's serializer and stamps the
    /// serializer's content type and payload format (unless already set explicitly).
    /// </summary>
    /// <exception cref="InvalidOperationException">No serializer is configured.</exception>
    public MqttPublishBuilder WithPayload<T>(T value)
    {
        var serializer = _client.SerializerOrThrow();
        _payload = serializer.Serialize(value);
        _contentType ??= serializer.ContentType;
        if (_payloadFormat == MqttPayloadFormatIndicator.Unspecified)
        {
            _payloadFormat = serializer.PayloadFormat;
        }

        return this;
    }

    /// <summary>Sets the MQTT 5 content type.</summary>
    public MqttPublishBuilder WithContentType(string contentType)
    {
        ArgumentException.ThrowIfNullOrEmpty(contentType);
        _contentType = contentType;
        return this;
    }

    /// <summary>Sets the MQTT 5 message expiry. Sub-second values round up to one second.</summary>
    public MqttPublishBuilder WithMessageExpiry(TimeSpan expiry)
    {
        if (expiry <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(expiry), expiry, "Expiry must be positive.");
        }

        _messageExpiryInterval = (uint)Math.Ceiling(expiry.TotalSeconds);
        return this;
    }

    /// <summary>Sets the MQTT 5 response topic for hand-rolled request patterns.</summary>
    public MqttPublishBuilder WithResponseTopic(string responseTopic)
    {
        ArgumentException.ThrowIfNullOrEmpty(responseTopic);
        _responseTopic = responseTopic;
        return this;
    }

    /// <summary>Sets the MQTT 5 correlation data.</summary>
    public MqttPublishBuilder WithCorrelationData(ReadOnlyMemory<byte> correlationData)
    {
        _correlationData = correlationData;
        return this;
    }

    /// <summary>Adds an MQTT 5 user property. Repeatable; duplicate names are allowed.</summary>
    public MqttPublishBuilder WithUserProperty(string name, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(value);
        (_userProperties ??= []).Add(new MqttUserProperty(name, value));
        return this;
    }

    /// <summary>Publishes the composed message and returns its outcome.</summary>
    public Task<PublishOutcome> SendAsync(CancellationToken cancellationToken = default)
    {
        var packet = new MqttPublishPacket
        {
            Topic = _topic,
            Payload = _payload,
            QualityOfService = _qualityOfService,
            Retain = _retain,
            ContentType = _contentType,
            PayloadFormatIndicator = _payloadFormat,
            MessageExpiryInterval = _messageExpiryInterval,
            ResponseTopic = _responseTopic,
            CorrelationData = _correlationData,
            UserProperties = _userProperties ?? (IReadOnlyList<MqttUserProperty>)[],
        };

        return _client.PublishAsync(packet, cancellationToken);
    }
}
