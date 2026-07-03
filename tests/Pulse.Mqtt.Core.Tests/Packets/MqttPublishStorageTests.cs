using System.Text;
using Pulse.Mqtt;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Packets;

public sealed class MqttPublishStorageTests
{
    [Theory]
    [InlineData(MqttQualityOfService.AtLeastOnce)]
    [InlineData(MqttQualityOfService.ExactlyOnce)]
    public void A_qos_publish_without_an_identifier_round_trips_through_storage_form(MqttQualityOfService qos)
    {
        // The offline queue holds a QoS > 0 publish before it is sent, so it has no identifier yet.
        var live = new MqttPublishPacket
        {
            Topic = "orders/1",
            Payload = Encoding.UTF8.GetBytes("hello"),
            QualityOfService = qos,
            PacketIdentifier = null,
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        var stored = MqttPublishStorage.ToStorageForm(live);
        stored.PacketIdentifier.ShouldBe((ushort)0); // reserved placeholder so the wire codec can encode it

        var restored = MqttPublishStorage.FromStorageForm(stored);
        restored.PacketIdentifier.ShouldBeNull(); // back to null: the client assigns one at send time
        restored.Topic.ShouldBe("orders/1");
        restored.QualityOfService.ShouldBe(qos);
    }

    [Fact]
    public void An_in_flight_publish_with_a_real_identifier_is_left_untouched()
    {
        // In-flight session packets are already sent and carry a real identifier that must survive.
        var live = new MqttPublishPacket
        {
            Topic = "orders/1",
            QualityOfService = MqttQualityOfService.AtLeastOnce,
            PacketIdentifier = 4242,
        };

        MqttPublishStorage.ToStorageForm(live).PacketIdentifier.ShouldBe((ushort)4242);
        MqttPublishStorage.FromStorageForm(live).PacketIdentifier.ShouldBe((ushort)4242);
    }

    [Fact]
    public void A_qos0_publish_is_left_untouched_in_both_directions()
    {
        var live = new MqttPublishPacket
        {
            Topic = "telemetry",
            QualityOfService = MqttQualityOfService.AtMostOnce,
            PacketIdentifier = null,
        };

        MqttPublishStorage.ToStorageForm(live).PacketIdentifier.ShouldBeNull();
        MqttPublishStorage.FromStorageForm(live).PacketIdentifier.ShouldBeNull();
    }
}
