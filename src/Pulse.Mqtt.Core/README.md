# Pulse.Mqtt.Core

Core protocol types, packet codecs, transports, stores, and extension contracts for the Pulse MQTT stack.

Install this package when you are building low-level MQTT integrations, custom transports, custom stores, serializers, or tools that need direct access to the MQTT packet model. Most applications should start with `Pulse.Mqtt.Client` instead.

## Install

```shell
dotnet add package Pulse.Mqtt.Core
```

## What is included

- MQTT 5.0 and MQTT 3.1.1 packet records and reason codes.
- Reader and writer code for MQTT control packets.
- TCP/TLS and in-memory loopback transport implementations.
- The raw client for direct CONNECT, PUBLISH, SUBSCRIBE, and acknowledgement flows.
- Contracts for reconnect decisions, session stores, message stores, serializers, transports, lifecycle hooks, and clocks.

## Basic raw client shape

```csharp
var transportFactory = new TcpTransportFactory(new TcpTransportOptions
{
    Host = "broker.example.com",
    Port = 1883,
});

await using var client = new RawMqttClient(transportFactory);

await client.ConnectAsync(new MqttConnectPacket
{
    ClientId = "raw-client",
}, cancellationToken);

await client.PublishAsync(new MqttPublishPacket
{
    Topic = "telemetry/device-7",
    Payload = Encoding.UTF8.GetBytes("online"),
}, cancellationToken);
```

## Related packages

- `Pulse.Mqtt.Client` adds the resilient high-level client, routing, typed messaging, and offline queue.
- `Pulse.Mqtt.Testing` uses the core transport contracts for in-process tests.
- Storage, serializer, transport, and resilience packages plug into the contracts defined here.

Full docs: https://araxis.github.io/pulse-mqtt/
