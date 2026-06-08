using Pulse.Mqtt;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Messages;

public sealed class MqttTopicFilterTests
{
    [Fact]
    public void ToSubscriptionOptions_packs_all_fields()
    {
        var filter = new MqttTopicFilter("sensors/+/temp")
        {
            MaximumQualityOfService = MqttQualityOfService.ExactlyOnce, // bits 0-1 = 10
            NoLocal = true,                                            // bit 2
            RetainAsPublished = true,                                  // bit 3
            RetainHandling = MqttRetainHandling.DoNotSendAtSubscribe,  // bits 4-5 = 10
        };

        // 10 (qos) | 1<<2 | 1<<3 | (2<<4) = 0x2E
        filter.ToSubscriptionOptions().ShouldBe((byte)0x2E);
    }

    [Fact]
    public void Default_filter_packs_to_zero()
    {
        new MqttTopicFilter("a/b").ToSubscriptionOptions().ShouldBe((byte)0x00);
    }

    [Theory]
    [InlineData((byte)0x00)]
    [InlineData((byte)0x2E)]
    [InlineData((byte)0x01)]
    [InlineData((byte)0x14)]
    public void Options_byte_round_trips(byte options)
    {
        var filter = MqttTopicFilter.FromSubscriptionOptions("topic", options);

        filter.ToSubscriptionOptions().ShouldBe(options);
    }

    [Fact]
    public void FromSubscriptionOptions_unpacks_fields()
    {
        var filter = MqttTopicFilter.FromSubscriptionOptions("topic", 0x2E);

        filter.MaximumQualityOfService.ShouldBe(MqttQualityOfService.ExactlyOnce);
        filter.NoLocal.ShouldBeTrue();
        filter.RetainAsPublished.ShouldBeTrue();
        filter.RetainHandling.ShouldBe(MqttRetainHandling.DoNotSendAtSubscribe);
    }
}
