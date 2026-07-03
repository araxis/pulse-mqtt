using global::MessagePack;
using global::MessagePack.Resolvers;
using Pulse.Mqtt;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Serialization.MessagePack.Tests;

[MessagePackObject]
public sealed record TelemetryReading
{
    [Key(0)]
    public required string DeviceId { get; init; }

    [Key(1)]
    public required double Value { get; init; }
}

public sealed class MessagePackMqttSerializerTests
{
    // Options built from the source-generated resolver (no dynamic codegen at runtime).
    private static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
        .WithResolver(CompositeResolver.Create(GeneratedMessagePackResolver.Instance, StandardResolver.Instance));

    private static MessagePackMqttSerializer NewSerializer() => new(Options);

    [Fact]
    public void Round_trips_through_the_generated_resolver()
    {
        var serializer = NewSerializer();
        var reading = new TelemetryReading { DeviceId = "d-1", Value = 21.5 };

        var payload = serializer.Serialize(reading);
        var roundTripped = serializer.Deserialize<TelemetryReading>(payload);

        roundTripped.ShouldBe(reading);
    }

    [Fact]
    public void Stamps_messagepack_metadata()
    {
        var serializer = NewSerializer();

        serializer.ContentType.ShouldBe("application/x-msgpack");
        serializer.PayloadFormat.ShouldBe(MqttPayloadFormatIndicator.Unspecified);
    }

    [Fact]
    public void Produces_compact_binary_smaller_than_json()
    {
        var serializer = NewSerializer();
        var reading = new TelemetryReading { DeviceId = "device-1234", Value = 21.5 };

        var messagePack = serializer.Serialize(reading);

        // The bytes are valid MessagePack: convert to JSON to confirm structure and content.
        var asJson = MessagePackSerializer.ConvertToJson(messagePack.ToArray(), Options);
        asJson.ShouldContain("device-1234");
        asJson.ShouldContain("21.5");

        // And materially smaller than the equivalent JSON object form.
        var jsonLength = System.Text.Encoding.UTF8.GetByteCount("{\"DeviceId\":\"device-1234\",\"Value\":21.5}");
        messagePack.Length.ShouldBeLessThan(jsonLength);
    }

    [Fact]
    public void Deserializing_garbage_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();
        var garbage = new byte[] { 0xC1, 0xFF, 0x00, 0x42 };

        var error = Should.Throw<MqttException>(() => serializer.Deserialize<TelemetryReading>(garbage));
        error.Message.ShouldContain(nameof(TelemetryReading));
    }

    [Fact]
    public void Deserializing_a_nil_payload_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();
        var nil = new byte[] { 0xC0 }; // valid MessagePack nil: deserializes to null, not a parse error

        // Before the fix this returned null (nil is valid MessagePack, so no
        // MessagePackSerializationException was thrown), leaking null into a non-nullable typed handler.
        var error = Should.Throw<MqttException>(() => serializer.Deserialize<TelemetryReading>(nil));
        error.Message.ShouldContain(nameof(TelemetryReading));
    }
}
