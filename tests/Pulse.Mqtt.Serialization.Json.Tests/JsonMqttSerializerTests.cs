using System.Text.Json.Serialization;
using Pulse.Mqtt;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Serialization.Json.Tests;

public sealed record TelemetryReading
{
    public required string DeviceId { get; init; }

    public required double Value { get; init; }
}

// Source-generated context, so serialization stays reflection-free (trimming / Native AOT safe).
[JsonSerializable(typeof(TelemetryReading))]
internal sealed partial class TestJsonContext : JsonSerializerContext
{
}

public sealed class JsonMqttSerializerTests
{
    private static JsonMqttSerializer NewSerializer() => new(TestJsonContext.Default);

    [Fact]
    public void Round_trips_through_the_generated_context()
    {
        var serializer = NewSerializer();
        var reading = new TelemetryReading { DeviceId = "d-1", Value = 21.5 };

        var payload = serializer.Serialize(reading);
        var roundTripped = serializer.Deserialize<TelemetryReading>(payload);

        roundTripped.ShouldBe(reading);
    }

    [Fact]
    public void Stamps_json_metadata()
    {
        var serializer = NewSerializer();

        serializer.ContentType.ShouldBe("application/json");
        serializer.PayloadFormat.ShouldBe(MqttPayloadFormatIndicator.Utf8);
    }

    [Fact]
    public void Deserializing_an_empty_payload_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();

        // A zero-byte payload (e.g. a retained-message clear) makes System.Text.Json throw
        // JsonException. Before the fix that raw JsonException escaped instead of the documented
        // MqttException, breaking uniform catch(MqttException) handling on request/response paths.
        var error = Should.Throw<MqttException>(() => serializer.Deserialize<TelemetryReading>(ReadOnlyMemory<byte>.Empty));
        error.Message.ShouldContain(nameof(TelemetryReading));
    }

    [Fact]
    public void Deserializing_malformed_json_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();
        var malformed = "{ not json"u8.ToArray();

        var error = Should.Throw<MqttException>(() => serializer.Deserialize<TelemetryReading>(malformed));
        error.Message.ShouldContain(nameof(TelemetryReading));
    }

    [Fact]
    public void Deserializing_a_json_null_literal_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();
        var jsonNull = "null"u8.ToArray();

        // A valid `null` literal deserializes to null; the type guard must still surface an MqttException.
        var error = Should.Throw<MqttException>(() => serializer.Deserialize<TelemetryReading>(jsonNull));
        error.Message.ShouldContain(nameof(TelemetryReading));
    }
}
