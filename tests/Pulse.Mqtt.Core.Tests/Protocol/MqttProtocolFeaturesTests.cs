using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Protocol;

public sealed class MqttProtocolFeaturesTests
{
    public static IEnumerable<object[]> AllFeatures() =>
        Enum.GetValues<MqttProtocolFeature>().Select(feature => new object[] { feature });

    [Theory]
    [MemberData(nameof(AllFeatures))]
    public void IsSupported_returns_true_for_mqtt5(MqttProtocolFeature feature)
    {
        MqttProtocolFeatures.IsSupported(MqttProtocolVersion.V500, feature).ShouldBeTrue();
    }

    [Theory]
    [MemberData(nameof(AllFeatures))]
    public void IsSupported_returns_false_for_mqtt311(MqttProtocolFeature feature)
    {
        MqttProtocolFeatures.IsSupported(MqttProtocolVersion.V311, feature).ShouldBeFalse();
    }

    [Fact]
    public void EnsureSupported_passes_for_supported_feature()
    {
        Should.NotThrow(() =>
            MqttProtocolFeatures.EnsureSupported(
                MqttProtocolVersion.V500,
                MqttProtocolFeature.RequestResponse,
                "RequestAsync"));
    }

    [Fact]
    public void EnsureSupported_throws_clear_message_for_unsupported_feature()
    {
        var exception = Should.Throw<NotSupportedException>(() =>
            MqttProtocolFeatures.EnsureSupported(
                MqttProtocolVersion.V311,
                MqttProtocolFeature.RequestResponse,
                "RequestAsync"));

        exception.Message.ShouldContain("RequestAsync");
        exception.Message.ShouldContain(nameof(MqttProtocolFeature.RequestResponse));
        exception.Message.ShouldContain(nameof(MqttProtocolVersion.V311));
    }

    [Fact]
    public void IsSupported_rejects_unknown_feature()
    {
        Should.Throw<ArgumentOutOfRangeException>(() =>
            MqttProtocolFeatures.IsSupported(MqttProtocolVersion.V500, (MqttProtocolFeature)999));
    }
}
