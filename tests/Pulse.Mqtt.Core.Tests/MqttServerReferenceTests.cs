using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests;

public sealed class MqttServerReferenceTests
{
    [Theory]
    [InlineData("backup.example", "backup.example", null)]
    [InlineData("backup.example:1884", "backup.example", 1884)]
    [InlineData("  backup.example:1884  ", "backup.example", 1884)]
    [InlineData("first.example:1884 second.example:1885", "first.example", 1884)]
    [InlineData("[2001:db8::1]", "2001:db8::1", null)]
    [InlineData("[2001:db8::1]:1884", "2001:db8::1", 1884)]
    [InlineData("2001:db8::1", "2001:db8::1", null)] // bare IPv6 cannot carry a port
    public void Parses_the_first_server_entry(string reference, string host, int? port)
    {
        MqttServerReference.TryParse(reference, out var parsedHost, out var parsedPort).ShouldBeTrue();
        parsedHost.ShouldBe(host);
        parsedPort.ShouldBe(port);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(":1884")]
    [InlineData("host:notaport")]
    [InlineData("host:0")]
    [InlineData("host:65536")]
    [InlineData("[unterminated:1884")]
    public void Rejects_unusable_references(string? reference)
    {
        MqttServerReference.TryParse(reference, out _, out _).ShouldBeFalse();
    }
}
