using System.Diagnostics;
using Pulse.Mqtt.Packets;

namespace Pulse.Mqtt.Client;

/// <summary>
/// W3C trace-context propagation over MQTT 5 user properties: a producer writes the active span's
/// <c>traceparent</c> (and <c>tracestate</c>) onto a publish; a consumer reads them back to parent
/// its receive span on the producer's span across processes.
/// </summary>
internal static class TraceContextPropagation
{
    private const string TraceParent = "traceparent";
    private const string TraceState = "tracestate";

    /// <summary>
    /// Returns <paramref name="packet"/> with a <c>traceparent</c> (and <c>tracestate</c>) user
    /// property carrying <paramref name="context"/>, unless the packet already has a traceparent
    /// (a caller-set value wins).
    /// </summary>
    public static MqttPublishPacket Inject(MqttPublishPacket packet, ActivityContext context)
    {
        foreach (var property in packet.UserProperties)
        {
            if (property.Name == TraceParent)
            {
                return packet;
            }
        }

        var flags = (context.TraceFlags & ActivityTraceFlags.Recorded) != 0 ? "01" : "00";
        var traceParent = $"00-{context.TraceId}-{context.SpanId}-{flags}";

        var properties = new List<MqttUserProperty>(packet.UserProperties.Count + 2);
        properties.AddRange(packet.UserProperties);
        properties.Add(new MqttUserProperty(TraceParent, traceParent));
        if (!string.IsNullOrEmpty(context.TraceState))
        {
            properties.Add(new MqttUserProperty(TraceState, context.TraceState));
        }

        return packet with { UserProperties = properties };
    }

    /// <summary>
    /// Extracts a remote <see cref="ActivityContext"/> from a message's user properties, or
    /// <see langword="null"/> when no valid <c>traceparent</c> is present.
    /// </summary>
    public static ActivityContext? Extract(IReadOnlyList<MqttUserProperty> userProperties)
    {
        string? traceParent = null;
        string? traceState = null;
        foreach (var property in userProperties)
        {
            if (property.Name == TraceParent)
            {
                traceParent = property.Value;
            }
            else if (property.Name == TraceState)
            {
                traceState = property.Value;
            }
        }

        if (traceParent is null)
        {
            return null;
        }

        return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var context) ? context : null;
    }
}
