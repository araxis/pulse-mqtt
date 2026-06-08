using Pulse.Mqtt;
using Pulse.Mqtt.Codec;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Codec;

public sealed class MqttPropertyTypesTests
{
    [Theory]
    [InlineData(MqttPropertyId.PayloadFormatIndicator, MqttPropertyType.Byte)]
    [InlineData(MqttPropertyId.SessionExpiryInterval, MqttPropertyType.FourByteInteger)]
    [InlineData(MqttPropertyId.ReceiveMaximum, MqttPropertyType.TwoByteInteger)]
    [InlineData(MqttPropertyId.ContentType, MqttPropertyType.Utf8String)]
    [InlineData(MqttPropertyId.CorrelationData, MqttPropertyType.BinaryData)]
    [InlineData(MqttPropertyId.SubscriptionIdentifier, MqttPropertyType.VariableByteInteger)]
    [InlineData(MqttPropertyId.UserProperty, MqttPropertyType.Utf8StringPair)]
    public void GetValueType_maps_known_ids(MqttPropertyId id, MqttPropertyType expected)
    {
        MqttPropertyTypes.GetValueType(id).ShouldBe(expected);
    }

    [Fact]
    public void GetValueType_throws_on_unknown_id()
    {
        Should.Throw<MqttProtocolException>(() => MqttPropertyTypes.GetValueType((MqttPropertyId)0x7F));
    }

    [Fact]
    public void AllowsMultiple_only_for_user_property_and_subscription_identifier()
    {
        MqttPropertyTypes.AllowsMultiple(MqttPropertyId.UserProperty).ShouldBeTrue();
        MqttPropertyTypes.AllowsMultiple(MqttPropertyId.SubscriptionIdentifier).ShouldBeTrue();
        MqttPropertyTypes.AllowsMultiple(MqttPropertyId.ContentType).ShouldBeFalse();
    }
}
