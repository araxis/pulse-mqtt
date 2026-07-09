# Pulse.Mqtt.Transport.WebSocket

MQTT over WebSocket transport for Pulse MQTT clients. Use it when the broker is exposed through `ws` or `wss`, often behind a gateway, reverse proxy, or managed endpoint.

## Install

```shell
dotnet add package Pulse.Mqtt.Transport.WebSocket
```

## Configure with dependency injection

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseTransportFactory(_ => new WebSocketTransportFactory(
        new WebSocketTransportOptions
        {
            Uri = new Uri("wss://broker.example.com/mqtt"),
        }));
```

The MQTT subprotocol defaults to `mqtt`.

## Configure directly

```csharp
var transportFactory = new WebSocketTransportFactory(
    new WebSocketTransportOptions
    {
        Uri = new Uri("wss://broker.example.com/mqtt"),
        Headers = new Dictionary<string, string>
        {
            ["Authorization"] = $"Bearer {token}",
        },
    });

await using var client = new ResilientMqttClient(
    transportFactory,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "telemetry-worker" },
    });
```

Each reconnect attempt gets a fresh WebSocket. MQTT packets are sent as binary frames.

Full docs: https://araxis.github.io/pulse-mqtt/packages/transport-websocket
