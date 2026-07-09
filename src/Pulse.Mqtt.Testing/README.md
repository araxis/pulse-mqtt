# Pulse.Mqtt.Testing

In-process MQTT test broker and loopback transport for fast client tests without Docker, ports, sleeps, or a production broker process.

## Install

```shell
dotnet add package Pulse.Mqtt.Testing
```

## Basic test broker

```csharp
await using var broker = new PulseMqttTestBroker();

await using var client = new ResilientMqttClient(
    broker,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "sut" },
    });

await client.ConnectAsync(cancellationToken);
await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(5), cancellationToken);
```

The broker implements `IMqttTransportFactory`, so production client construction does not need a test-only adapter.

## Dependency-injection tests

```csharp
await using var broker = new PulseMqttTestBroker();

builder.Services
    .AddPulseMqttClient("sut", options =>
    {
        options.Host = "unused";
        options.ClientId = "sut";
    })
    .UseTransportFactory(_ => broker);
```

## Script broker behavior

```csharp
await using var broker = new PulseMqttTestBroker(new PulseMqttTestBrokerOptions
{
    SubAckFactory = context => context.DefaultSubAck with
    {
        ReasonCodes = [MqttReasonCode.Success, MqttReasonCode.NotAuthorized],
    },
    PublishAckFactory = context => context.DefaultAcknowledgement,
});
```

The package is for application tests, not a production broker.

Full docs: https://araxis.github.io/pulse-mqtt/packages/testing
