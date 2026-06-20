namespace Pulse.Mqtt.Routing;

/// <summary>
/// A route template over MQTT topics: literal levels, MQTT wildcards (<c>+</c>, trailing
/// <c>#</c>), and named parameters such as <c>{deviceId}</c> that capture one level. A template
/// converts to a plain MQTT filter for SUBSCRIBE via <see cref="TopicFilter"/>.
/// </summary>
public sealed class MqttRouteTemplate
{
    private enum SegmentKind
    {
        Literal,
        Parameter,
        SingleLevelWildcard,
        MultiLevelWildcard,
    }

    private readonly string[] _segments;
    private readonly SegmentKind[] _kinds;
    private readonly string[] _parameterNames;

    private MqttRouteTemplate(string template, string topicFilter, string[] segments, SegmentKind[] kinds, string[] parameterNames)
    {
        Template = template;
        TopicFilter = topicFilter;
        _segments = segments;
        _kinds = kinds;
        _parameterNames = parameterNames;
    }

    /// <summary>The original template text.</summary>
    public string Template { get; }

    /// <summary>The MQTT subscription filter for this template (<c>{param}</c> becomes <c>+</c>).</summary>
    public string TopicFilter { get; }

    /// <summary>The parameter names in template order.</summary>
    public IReadOnlyList<string> ParameterNames => _parameterNames;

    /// <summary>Creates a broker subscription filter from this route template.</summary>
    public global::Pulse.Mqtt.MqttTopicFilter ToTopicFilter(
        global::Pulse.Mqtt.MqttQualityOfService maximumQualityOfService = global::Pulse.Mqtt.MqttQualityOfService.AtMostOnce,
        bool noLocal = false,
        bool retainAsPublished = false,
        global::Pulse.Mqtt.MqttRetainHandling retainHandling = global::Pulse.Mqtt.MqttRetainHandling.SendAtSubscribe) =>
        new(TopicFilter)
        {
            MaximumQualityOfService = maximumQualityOfService,
            NoLocal = noLocal,
            RetainAsPublished = retainAsPublished,
            RetainHandling = retainHandling,
        };

    /// <summary>Parses <paramref name="template"/>.</summary>
    /// <exception cref="ArgumentException">The template is malformed.</exception>
    public static MqttRouteTemplate Parse(string template)
    {
        ArgumentException.ThrowIfNullOrEmpty(template);

        var rawSegments = template.Split('/');
        var segments = new string[rawSegments.Length];
        var kinds = new SegmentKind[rawSegments.Length];
        var parameterNames = new List<string>();

        for (var i = 0; i < rawSegments.Length; i++)
        {
            var segment = rawSegments[i];

            if (segment == "#")
            {
                if (i != rawSegments.Length - 1)
                {
                    throw new ArgumentException($"'#' is only valid as the last level: '{template}'.", nameof(template));
                }

                kinds[i] = SegmentKind.MultiLevelWildcard;
                segments[i] = segment;
                continue;
            }

            if (segment == "+")
            {
                kinds[i] = SegmentKind.SingleLevelWildcard;
                segments[i] = segment;
                continue;
            }

            if (segment.StartsWith('{') && segment.EndsWith('}') && segment.Length > 2)
            {
                var name = segment[1..^1];
                if (name.Contains('{') || name.Contains('}'))
                {
                    throw new ArgumentException($"Malformed parameter segment '{segment}' in '{template}'.", nameof(template));
                }

                if (parameterNames.Contains(name))
                {
                    throw new ArgumentException($"Duplicate parameter name '{name}' in '{template}'.", nameof(template));
                }

                parameterNames.Add(name);
                kinds[i] = SegmentKind.Parameter;
                segments[i] = name;
                continue;
            }

            if (segment.Contains('{') || segment.Contains('}') || segment.Contains('+') || segment.Contains('#'))
            {
                throw new ArgumentException(
                    $"Segment '{segment}' in '{template}' mixes literals with wildcard or parameter syntax.",
                    nameof(template));
            }

            kinds[i] = SegmentKind.Literal;
            segments[i] = segment;
        }

        var filterSegments = new string[rawSegments.Length];
        for (var i = 0; i < rawSegments.Length; i++)
        {
            filterSegments[i] = kinds[i] switch
            {
                SegmentKind.Parameter => "+",
                _ => segments[i],
            };
        }

        return new MqttRouteTemplate(template, string.Join('/', filterSegments), segments, kinds, [.. parameterNames]);
    }

    /// <summary>Attempts to match <paramref name="topic"/>, capturing parameter values on success.</summary>
    public bool TryMatch(string topic, out MqttRouteValues values)
    {
        ArgumentNullException.ThrowIfNull(topic);
        values = MqttRouteValues.Empty;

        // Wildcard/parameter templates must not match $-prefixed topics, mirroring filter rules.
        if (topic.StartsWith('$') && _kinds[0] != SegmentKind.Literal)
        {
            return false;
        }

        var captured = _parameterNames.Length > 0 ? new string[_parameterNames.Length] : null;
        var captureIndex = 0;

        ReadOnlySpan<char> remaining = topic;
        for (var i = 0; i < _segments.Length; i++)
        {
            if (_kinds[i] == SegmentKind.MultiLevelWildcard)
            {
                Capture(captured, ref values);
                return true; // matches the remainder, however deep — including the parent level
            }

            var slash = remaining.IndexOf('/');
            var segment = slash < 0 ? remaining : remaining[..slash];

            var isLastTemplateSegment = i == _segments.Length - 1;
            var isLastTopicSegment = slash < 0;

            switch (_kinds[i])
            {
                case SegmentKind.Literal when !segment.SequenceEqual(_segments[i]):
                    return false;
                case SegmentKind.Parameter:
                    captured![captureIndex++] = segment.ToString();
                    break;
            }

            if (isLastTemplateSegment)
            {
                if (!isLastTopicSegment)
                {
                    return false; // the topic continues past the template
                }

                Capture(captured, ref values);
                return true;
            }

            if (isLastTopicSegment)
            {
                // The topic ended but the template continues: only a trailing '#' matches the parent.
                return _kinds[i + 1] == SegmentKind.MultiLevelWildcard && i + 1 == _segments.Length - 1
                    && CaptureAndSucceed(captured, ref values);
            }

            remaining = remaining[(slash + 1)..];
        }

        return false;

        void Capture(string[]? capturedValues, ref MqttRouteValues result)
        {
            if (capturedValues is not null)
            {
                result = new MqttRouteValues(_parameterNames, capturedValues);
            }
        }

        bool CaptureAndSucceed(string[]? capturedValues, ref MqttRouteValues result)
        {
            Capture(capturedValues, ref result);
            return true;
        }
    }
}
