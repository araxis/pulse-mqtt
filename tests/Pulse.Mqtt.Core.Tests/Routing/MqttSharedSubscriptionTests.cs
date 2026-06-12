using Pulse.Mqtt.Routing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Routing;

public sealed class MqttSharedSubscriptionTests
{
    [Fact]
    public void Formats_the_share_filter()
    {
        MqttSharedSubscription.Format("workers", "jobs/+/created").ShouldBe("$share/workers/jobs/+/created");
    }

    [Theory]
    [InlineData("")]
    [InlineData("a/b")]
    [InlineData("a+")]
    [InlineData("a#")]
    public void Rejects_invalid_group_names(string group)
    {
        Should.Throw<ArgumentException>(() => MqttSharedSubscription.Format(group, "t"));
    }
}
