using System.Diagnostics;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Payload-level trace envelope for protocols or deployments that cannot carry MQTT 5 user
/// properties. Both producer and consumer must agree to serialize this envelope as the message
/// payload.
/// </summary>
public sealed class MqttTraceEnvelope<T>
{
    /// <summary>Creates an empty envelope for serializers that need a parameterless constructor.</summary>
    public MqttTraceEnvelope()
    {
    }

    /// <summary>Creates an envelope with an optional trace context.</summary>
    public MqttTraceEnvelope(T payload, string? traceParent = null, string? traceState = null)
    {
        Payload = payload;
        TraceParent = traceParent;
        TraceState = traceState;
    }

    /// <summary>The application payload.</summary>
    public T Payload { get; init; } = default!;

    /// <summary>The W3C <c>traceparent</c> value, when a producer activity was available.</summary>
    public string? TraceParent { get; init; }

    /// <summary>The W3C <c>tracestate</c> value, when present on the producer activity.</summary>
    public string? TraceState { get; init; }

    /// <summary>Creates an envelope using the current <see cref="Activity"/> context, when one exists.</summary>
    public static MqttTraceEnvelope<T> Create(T payload) =>
        Activity.Current is { } current && TraceContextPropagation.IsValid(current.Context)
            ? Create(payload, current.Context)
            : new MqttTraceEnvelope<T>(payload);

    /// <summary>Creates an envelope using an explicit activity context.</summary>
    public static MqttTraceEnvelope<T> Create(T payload, ActivityContext context) =>
        TraceContextPropagation.IsValid(context)
            ? new MqttTraceEnvelope<T>(
                payload,
                TraceContextPropagation.FormatTraceParent(context),
                string.IsNullOrEmpty(context.TraceState) ? null : context.TraceState)
            : new MqttTraceEnvelope<T>(payload);

    /// <summary>Extracts the remote producer context from this envelope, when one is present and valid.</summary>
    public ActivityContext? ExtractContext() => TraceContextPropagation.Extract(TraceParent, TraceState);

    /// <summary>
    /// Starts a consumer activity parented to the envelope's remote context. Returns
    /// <see langword="null"/> when no valid context is present or no listener is attached.
    /// </summary>
    public Activity? StartConsumerActivity(string topic)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        if (ExtractContext() is not { } remote)
        {
            return null;
        }

        var activity = PulseMqttDiagnostics.ActivitySource.StartActivity("receive", ActivityKind.Consumer, remote);
        if (activity is not null)
        {
            activity.DisplayName = $"receive {topic}";
            activity.SetTag("messaging.system", "mqtt");
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation.type", "process");
        }

        return activity;
    }
}
