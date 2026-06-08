using System.Text;
using Pulse.Mqtt;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Messages;

public sealed class MqttApplicationMessageTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var message = new MqttApplicationMessage("devices/1/telemetry");

        message.Topic.ShouldBe("devices/1/telemetry");
        message.QualityOfService.ShouldBe(MqttQualityOfService.AtMostOnce);
        message.Retain.ShouldBeFalse();
        message.Payload.Length.ShouldBe(0);
        message.PayloadFormatIndicator.ShouldBe(MqttPayloadFormatIndicator.Unspecified);
        message.UserProperties.ShouldBeEmpty();
        message.SubscriptionIdentifiers.ShouldBeEmpty();
        message.MessageExpiryInterval.ShouldBeNull();
    }

    [Fact]
    public void Init_properties_and_with_expression_work()
    {
        var message = new MqttApplicationMessage("t")
        {
            Payload = Encoding.UTF8.GetBytes("hello"),
            QualityOfService = MqttQualityOfService.AtLeastOnce,
            Retain = true,
            ContentType = "text/plain",
            UserProperties = [new MqttUserProperty("k", "v")],
        };

        message.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        message.Retain.ShouldBeTrue();
        message.ContentType.ShouldBe("text/plain");
        Encoding.UTF8.GetString(message.Payload.Span).ShouldBe("hello");
        message.UserProperties.ShouldHaveSingleItem().Name.ShouldBe("k");

        var requeued = message with { Retain = false };
        requeued.Retain.ShouldBeFalse();
        requeued.ContentType.ShouldBe("text/plain");
    }
}
