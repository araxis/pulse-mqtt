using System.Diagnostics;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Client;

/// <summary>Helpers for explicit payload-level trace propagation.</summary>
public static class MqttTraceEnvelopeExtensions
{
    /// <summary>
    /// Publishes <paramref name="payload"/> wrapped in a <see cref="MqttTraceEnvelope{T}"/>. The
    /// envelope captures the publish activity's context when tracing is active.
    /// </summary>
    public static Task<PublishOutcome> PublishWithTraceEnvelopeAsync<T>(
        this ResilientMqttClient client,
        string topic,
        T payload,
        MqttQualityOfService qualityOfService = MqttQualityOfService.AtMostOnce,
        bool retain = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(topic);
        var serializer = client.SerializerOrThrow();

        var packet = new MqttPublishPacket
        {
            Topic = topic,
            QualityOfService = qualityOfService,
            Retain = retain,
            ContentType = serializer.ContentType,
            PayloadFormatIndicator = serializer.PayloadFormat,
        };

        return client.PublishAsync(
            packet,
            (current, context) =>
            {
                var envelope = context is { } active && TraceContextPropagation.IsValid(active)
                    ? MqttTraceEnvelope<T>.Create(payload, active)
                    : new MqttTraceEnvelope<T>(payload);
                return current with { Payload = serializer.Serialize(envelope) };
            },
            cancellationToken);
    }

    /// <summary>
    /// Registers a route whose payload is a <see cref="MqttTraceEnvelope{T}"/>. The handler runs
    /// under a consumer activity parented to the envelope context when one is present.
    /// </summary>
    public static IDisposable RegisterTraceEnvelopeRoute<T>(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttTypedRouteHandler<T> handler) =>
        RegisterTraceEnvelopeRoute(client, template, handler, options: null);

    /// <summary>
    /// Registers a route whose payload is a <see cref="MqttTraceEnvelope{T}"/>. The handler runs
    /// under a consumer activity parented to the envelope context when one is present.
    /// </summary>
    public static IDisposable RegisterTraceEnvelopeRoute<T>(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttTypedRouteHandler<T> handler,
        MqttRouteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(handler);

        return client.RegisterRoute<MqttTraceEnvelope<T>>(
            template,
            (envelope, message, token) =>
            {
                using var activity = envelope.StartConsumerActivity(message.Message.Topic);
                return handler(envelope.Payload, message, token);
            },
            options);
    }

    /// <summary>
    /// Registers a route whose payload is a <see cref="MqttTraceEnvelope{T}"/>. The handler runs
    /// under a consumer activity parented to the envelope context when one is present.
    /// </summary>
    public static IDisposable RegisterTraceEnvelopeRoute<T>(
        this ResilientMqttClient client,
        string template,
        MqttTypedRouteHandler<T> handler) =>
        RegisterTraceEnvelopeRoute(client, MqttRouteTemplate.Parse(template), handler, options: null);

    /// <summary>
    /// Registers a route whose payload is a <see cref="MqttTraceEnvelope{T}"/>. The handler runs
    /// under a consumer activity parented to the envelope context when one is present.
    /// </summary>
    public static IDisposable RegisterTraceEnvelopeRoute<T>(
        this ResilientMqttClient client,
        string template,
        MqttTypedRouteHandler<T> handler,
        MqttRouteOptions? options) =>
        RegisterTraceEnvelopeRoute(client, MqttRouteTemplate.Parse(template), handler, options);
}
