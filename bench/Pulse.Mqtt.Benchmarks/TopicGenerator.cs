namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Builds per-publisher topic sets shaped like a building hierarchy
/// (<c>pub0/building1/level2/sensor3</c>), spreading the requested count across three levels.
/// </summary>
public static class TopicGenerator
{
    public static Dictionary<string, List<string>> GenerateTopicsByPublisher(int publisherCount, int topicsPerPublisher)
    {
        var topicsByPublisher = new Dictionary<string, List<string>>();

        var topicsPerLevel = Math.Max(1, (int)Math.Pow(topicsPerPublisher, 1.0 / 3.0));
        var level1Count = topicsPerLevel;
        var level2Count = topicsPerLevel;
        var level3Count = Math.Max(1, 1 + topicsPerPublisher / (level1Count * level2Count));

        for (var p = 0; p < publisherCount; p++)
        {
            var publisherName = "pub" + p;
            var topics = new List<string>(topicsPerPublisher);
            topicsByPublisher[publisherName] = topics;

            for (var l1 = 0; l1 < level1Count && topics.Count < topicsPerPublisher; l1++)
            {
                for (var l2 = 0; l2 < level2Count && topics.Count < topicsPerPublisher; l2++)
                {
                    for (var l3 = 0; l3 < level3Count && topics.Count < topicsPerPublisher; l3++)
                    {
                        topics.Add($"{publisherName}/building{l1 + 1}/level{l2 + 1}/sensor{l3 + 1}");
                    }
                }
            }
        }

        return topicsByPublisher;
    }
}
