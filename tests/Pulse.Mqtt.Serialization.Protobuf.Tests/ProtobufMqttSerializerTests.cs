using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Testing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Serialization.Protobuf.Tests;

public sealed class ProtobufMqttSerializerTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    private static ProtobufMqttSerializer NewSerializer() =>
        new(ProtobufMessageRegistry.Create(registry =>
        {
            registry.Add(StringValue.Parser);
            registry.Add(Int32Value.Parser);
        }));

    [Fact]
    public void Round_trips_through_a_registered_parser()
    {
        var serializer = NewSerializer();
        var value = new StringValue { Value = "device-1" };

        var payload = serializer.Serialize(value);
        var roundTripped = serializer.Deserialize<StringValue>(payload);

        roundTripped.Value.ShouldBe("device-1");
    }

    [Fact]
    public void Stamps_protobuf_metadata()
    {
        var serializer = NewSerializer();

        serializer.ContentType.ShouldBe("application/x-protobuf");
        serializer.PayloadFormat.ShouldBe(MqttPayloadFormatIndicator.Unspecified);
    }

    [Fact]
    public void Deserializing_garbage_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();
        var garbage = new byte[] { 0xFF, 0xFF, 0xFF };

        var error = Should.Throw<MqttException>(() => serializer.Deserialize<StringValue>(garbage));
        error.Message.ShouldContain(nameof(StringValue));
        error.InnerException.ShouldBeOfType<InvalidProtocolBufferException>();
    }

    [Fact]
    public void Serializing_non_protobuf_values_throws_a_clear_mqtt_exception()
    {
        var serializer = NewSerializer();

        var error = Should.Throw<MqttException>(() => serializer.Serialize(new PlainReading("d-1", 42.0)));
        error.Message.ShouldContain(nameof(PlainReading));
    }

    [Fact]
    public void Deserializing_without_a_registered_parser_throws_a_clear_mqtt_exception()
    {
        var serializer = new ProtobufMqttSerializer(ProtobufMessageRegistry.Empty);
        var payload = new StringValue { Value = "device-1" }.ToByteArray();

        var error = Should.Throw<MqttException>(() => serializer.Deserialize<StringValue>(payload));
        error.Message.ShouldContain(nameof(StringValue));
        error.Message.ShouldContain("registered");
    }

    [Fact]
    public void Multiple_registered_types_resolve_independently()
    {
        var serializer = NewSerializer();

        var textPayload = serializer.Serialize(new StringValue { Value = "online" });
        var numberPayload = serializer.Serialize(new Int32Value { Value = 42 });

        serializer.Deserialize<StringValue>(textPayload).Value.ShouldBe("online");
        serializer.Deserialize<Int32Value>(numberPayload).Value.ShouldBe(42);
    }

    [Fact]
    public async Task Typed_publish_uses_the_protobuf_serializer_without_client_api_changes()
    {
        await using var broker = new PulseMqttTestBroker();
        var serializer = NewSerializer();
        await using var client = new ResilientMqttClient(
            broker,
            new ResilientMqttClientOptions
            {
                Connect = new MqttConnectPacket { ClientId = "protobuf-publisher", KeepAliveSeconds = 0 },
                Serializer = serializer,
            });

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await client.PublishAsync(
            "protobuf/status",
            new StringValue { Value = "ready" },
            MqttQualityOfService.AtLeastOnce,
            cancellationToken: timeout.Token);

        var published = await broker.ClientPublishes.ReadAsync(timeout.Token);
        published.Topic.ShouldBe("protobuf/status");
        published.ContentType.ShouldBe("application/x-protobuf");
        published.PayloadFormatIndicator.ShouldBe(MqttPayloadFormatIndicator.Unspecified);
        serializer.Deserialize<StringValue>(published.Payload).Value.ShouldBe("ready");
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken ct)
    {
        while (client.State != state)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Delay(5, ct);
        }
    }

    private sealed record PlainReading(string DeviceId, double Value);
}
