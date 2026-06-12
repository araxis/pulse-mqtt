using BenchmarkDotNet.Attributes;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Topic filter matching throughput: eight representative comparisons (wildcards, deep levels,
/// roots, and one very long literal topic), 100,000 rounds per operation.
/// </summary>
[MemoryDiagnoser]
public class TopicFilterComparerBenchmarks
{
    private const string LongTopic =
        "AAAAAAAAAAAAAssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

    [Benchmark]
    public void Match_100_000_Rounds()
    {
        for (var i = 0; i < 100_000; i++)
        {
            MqttTopicFilterMatcher.Matches("sport/#", "sport/tennis/player1");
            MqttTopicFilterMatcher.Matches("sport/#/ranking", "sport/tennis/player1/ranking");
            MqttTopicFilterMatcher.Matches("sport/+/player1/#", "sport/tennis/player1/score/wimbledon");
            MqttTopicFilterMatcher.Matches("sport/tennis/+", "sport/tennis/player1");
            MqttTopicFilterMatcher.Matches("+/+", "/finance");
            MqttTopicFilterMatcher.Matches("/+", "/finance");
            MqttTopicFilterMatcher.Matches("+", "/finance");
            MqttTopicFilterMatcher.Matches(LongTopic, LongTopic);
        }
    }
}
