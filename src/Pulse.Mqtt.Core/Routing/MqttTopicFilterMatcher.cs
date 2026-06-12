namespace Pulse.Mqtt.Routing;

/// <summary>
/// Matches concrete topic names against MQTT topic filters per the specification's rules:
/// <c>+</c> matches exactly one level (including an empty one), <c>#</c> matches the remainder
/// including the parent level, and wildcard filters never match topics beginning with <c>$</c>.
/// Matching is span-based and allocation-free.
/// </summary>
public static class MqttTopicFilterMatcher
{
    /// <summary>Returns whether <paramref name="topic"/> matches <paramref name="filter"/>.</summary>
    public static bool Matches(ReadOnlySpan<char> filter, ReadOnlySpan<char> topic)
    {
        // A wildcard at the first level must not match $-prefixed (server-reserved) topics.
        if (!topic.IsEmpty && topic[0] == '$' && !filter.IsEmpty && (filter[0] == '+' || filter[0] == '#'))
        {
            return false;
        }

        while (true)
        {
            var filterSlash = filter.IndexOf('/');
            var filterSegment = filterSlash < 0 ? filter : filter[..filterSlash];

            if (filterSegment.Length == 1 && filterSegment[0] == '#')
            {
                return true; // matches the remainder, however deep
            }

            var topicSlash = topic.IndexOf('/');
            var topicSegment = topicSlash < 0 ? topic : topic[..topicSlash];

            var segmentMatches = (filterSegment.Length == 1 && filterSegment[0] == '+')
                || filterSegment.SequenceEqual(topicSegment);
            if (!segmentMatches)
            {
                return false;
            }

            var filterDone = filterSlash < 0;
            var topicDone = topicSlash < 0;

            if (filterDone && topicDone)
            {
                return true;
            }

            if (topicDone)
            {
                // The topic ended but the filter continues: only a trailing "/#" matches the parent.
                var rest = filter[(filterSlash + 1)..];
                return rest.Length == 1 && rest[0] == '#';
            }

            if (filterDone)
            {
                return false; // the filter ended but the topic continues
            }

            filter = filter[(filterSlash + 1)..];
            topic = topic[(topicSlash + 1)..];
        }
    }
}
