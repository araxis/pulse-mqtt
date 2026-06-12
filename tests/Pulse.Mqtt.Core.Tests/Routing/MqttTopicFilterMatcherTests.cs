using Pulse.Mqtt.Routing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Routing;

public sealed class MqttTopicFilterMatcherTests
{
    [Theory]
    // Exact literals
    [InlineData("a/b/c", "a/b/c", true)]
    [InlineData("a/b/c", "a/b/d", false)]
    [InlineData("a/b", "a/b/c", false)]
    [InlineData("a/b/c", "a/b", false)]
    // Single-level wildcard
    [InlineData("a/+/c", "a/b/c", true)]
    [InlineData("a/+/c", "a/x/c", true)]
    [InlineData("a/+/c", "a/b/x", false)]
    [InlineData("a/+", "a/b", true)]
    [InlineData("a/+", "a", false)]
    [InlineData("a/+", "a/", true)]        // '+' matches an empty level
    [InlineData("+", "a", true)]
    [InlineData("+/+", "a/b", true)]
    [InlineData("+", "a/b", false)]
    // Multi-level wildcard
    [InlineData("#", "a", true)]
    [InlineData("#", "a/b/c", true)]
    [InlineData("a/#", "a/b/c", true)]
    [InlineData("a/#", "a", true)]         // '#' also matches the parent level
    [InlineData("a/#", "b/c", false)]
    [InlineData("a/b/#", "a/b", true)]
    [InlineData("a/b/#", "a", false)]
    // $-prefixed (server-reserved) topics
    [InlineData("#", "$SYS/broker", false)]
    [InlineData("+/broker", "$SYS/broker", false)]
    [InlineData("$SYS/#", "$SYS/broker", true)]
    [InlineData("$SYS/broker", "$SYS/broker", true)]
    // Empty levels
    [InlineData("a//c", "a//c", true)]
    [InlineData("a/+/c", "a//c", true)]
    public void Matches_follows_the_specification(string filter, string topic, bool expected)
    {
        MqttTopicFilterMatcher.Matches(filter, topic).ShouldBe(expected);
    }
}
