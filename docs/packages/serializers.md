# Serializer package overview

Typed publish, route, stream, and request/response APIs use one `IMqttSerializer` per client.
Choose one serializer package for each named client and keep mixed wire formats on separate
clients or on the raw `MqttPublishPacket` APIs.

| Package | Page | Use when |
| --- | --- | --- |
| `Pulse.Mqtt.Serialization.Json` | [JSON serializer](./serialization-json) | Payloads should be human-readable UTF-8 JSON and Native AOT safe through source generation. |
| `Pulse.Mqtt.Serialization.MessagePack` | [MessagePack serializer](./serialization-messagepack) | Payloads should be compact binary data while keeping generated resolver support. |
| `Pulse.Mqtt.Serialization.Protobuf` | [Protobuf serializer](./serialization-protobuf) | Payloads are generated Protocol Buffers messages with explicit parser registration. |

All serializer packages plug into the same client option or dependency-injection swap:

```csharp
.UseSerializer(_ => serializer)
```

```csharp
new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "worker" },
    Serializer = serializer,
};
```

The serializer stamps MQTT payload metadata on typed publishes. Consumers can inspect content
type and payload format even when they are not using Pulse.Mqtt.

See [Typed messaging](/guide/typed-messaging) for end-to-end publish, route, stream, and
request/response examples.
