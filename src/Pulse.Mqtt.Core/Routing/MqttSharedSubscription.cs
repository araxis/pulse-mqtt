namespace Pulse.Mqtt.Routing;

/// <summary>Builds MQTT 5 shared-subscription filters for load-balanced consumer groups.</summary>
public static class MqttSharedSubscription
{
    /// <summary>Formats <c>$share/&lt;group&gt;/&lt;filter&gt;</c>.</summary>
    /// <exception cref="ArgumentException">The group is empty or contains separators or wildcards.</exception>
    public static string Format(string group, string topicFilter)
    {
        ArgumentException.ThrowIfNullOrEmpty(group);
        ArgumentException.ThrowIfNullOrEmpty(topicFilter);
        if (group.AsSpan().IndexOfAny('/', '+', '#') >= 0)
        {
            throw new ArgumentException("A share group name must not contain '/', '+', or '#'.", nameof(group));
        }

        return $"$share/{group}/{topicFilter}";
    }
}
