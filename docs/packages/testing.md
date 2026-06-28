# Testing package

Package: `Pulse.Mqtt.Testing`

Use this package for fast MQTT workflow tests without Docker, ports, sleeps, or a production
broker process.

## Install

```shell
dotnet add package Pulse.Mqtt.Testing
```

## What it provides

| Type | Purpose |
| --- | --- |
| `PulseMqttTestBroker` | In-process MQTT broker and `IMqttTransportFactory`. |
| `PulseMqttTestBrokerOptions` | Enables retained messages, persistent sessions, QoS 2 forwarding, and scripted responses. |
| `PulseMqttTestBrokerConnectContext` | CONNECT context passed to `ConnAckFactory`. |
| `PulseMqttTestBrokerSubscribeContext` | SUBSCRIBE context passed to `SubAckFactory`. |
| `PulseMqttTestBrokerPublishContext` | PUBLISH context passed to `PublishAckFactory`. |

## Basic test broker

```csharp
await using var broker = new PulseMqttTestBroker();

await using var client = new ResilientMqttClient(
    broker,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "sut" },
    });

await client.ConnectAsync(token);
await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(5), token);
```

The broker is an `IMqttTransportFactory`, so production client construction does not need a
testing-only adapter.

## Dependency-injection tests

```csharp
await using var broker = new PulseMqttTestBroker();

var services = new ServiceCollection();
services.AddPulseMqttClient("sut", options =>
{
    options.Host = "unused";
    options.ClientId = "sut";
})
    .UseTransportFactory(_ => broker);
```

## Protocol realism options

Enable only the behavior needed by the test:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    RetainedMessages = true,
    PersistentSessions = true,
    MaximumForwardQualityOfService = MqttQualityOfService.ExactlyOnce,
});
```

Defaults stay lightweight: no retained storage, no real session persistence, and broker
forwarding capped at QoS 1.

## Scripted responses

Script broker behavior without changing production client code:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    ConnAckFactory = context => context.DefaultConnAck with
    {
        ReasonString = "test connection accepted",
    },
    SubAckFactory = context => context.DefaultSubAck with
    {
        ReasonCodes = [MqttReasonCode.Success, MqttReasonCode.NotAuthorized],
    },
    PublishAckFactory = context => context.DefaultAcknowledgement,
});
```

Return a non-success CONNACK to reject a connection, deny individual subscription filters with a
SUBACK reason code, or return `null` from `PublishAckFactory` to withhold a publish
acknowledgement for timeout tests.

## Broker-side disconnects

```csharp
await broker.DisconnectClientAsync(
    "sut",
    new MqttDisconnectPacket
    {
        ReasonCode = MqttReasonCode.ServerBusy,
        ReasonString = "maintenance",
    },
    token);
```

`DisconnectAllAsync` drops every connected session. Resilient clients observe the loss through
the normal reconnect path.

## Operational notes

- The package is for tests, not production broker conformance.
- Retained messages and persistent sessions are in-memory and broker-local.
- Route source blocks and client APIs behave the same as they do with a network transport.
- Use explicit timeouts in tests so failed expectations do not hang.

## Related docs

- [Testing guide](/guide/testing)
- [Resilience tests](/guide/resilience)
- [Package add-ons](/guide/package-add-ons)
