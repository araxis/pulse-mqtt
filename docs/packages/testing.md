# Testing package

Package: `Pulse.Mqtt.Testing`

Use this package for fast MQTT workflow tests without Docker, ports, or sleeps.

## Install

```shell
dotnet add package Pulse.Mqtt.Testing
```

## In-process broker

```csharp
await using var broker = new PulseMqttTestBroker();

await using var client = new ResilientMqttClient(broker, new ResilientMqttClientOptions
{
    Connect = new MqttConnectPacket { ClientId = "sut" },
});

await client.ConnectAsync(token);
```

Enable protocol-realistic behavior only when a test needs it:

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    RetainedMessages = true,
    PersistentSessions = true,
    MaximumForwardQualityOfService = MqttQualityOfService.ExactlyOnce,
});
```

The broker is an `IMqttTransportFactory`, so it plugs into dependency injection with
`UseTransportFactory`.

See [Testing](/guide/testing) for retained messages, persistent sessions, QoS forwarding, and
deterministic time patterns.
