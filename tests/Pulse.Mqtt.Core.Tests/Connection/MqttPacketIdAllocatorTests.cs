using Pulse.Mqtt;
using Pulse.Mqtt.Connection;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Connection;

public sealed class MqttPacketIdAllocatorTests
{
    [Fact]
    public void Rents_distinct_identifiers()
    {
        var allocator = new MqttPacketIdAllocator();

        var first = allocator.Rent();
        var second = allocator.Rent();

        first.ShouldNotBe(second);
        first.ShouldNotBe((ushort)0);
        second.ShouldNotBe((ushort)0);
    }

    [Fact]
    public void Returned_identifier_can_be_rented_again()
    {
        var allocator = new MqttPacketIdAllocator();

        var id = allocator.Rent();
        allocator.Return(id);

        // Drain the rest of the space; the returned id must come around again instead of throwing.
        for (var i = 0; i < ushort.MaxValue; i++)
        {
            allocator.Rent();
        }
    }

    [Fact]
    public void Throws_when_exhausted()
    {
        var allocator = new MqttPacketIdAllocator();
        for (var i = 0; i < ushort.MaxValue; i++)
        {
            allocator.Rent();
        }

        Should.Throw<MqttException>(() => allocator.Rent());
    }
}
