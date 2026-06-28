# WebSocket transport package

Package: `Pulse.Mqtt.Transport.WebSocket`

Use this package when the broker is exposed through `ws` or `wss`, often behind a reverse proxy,
gateway, or managed broker endpoint.

## Install

```shell
dotnet add package Pulse.Mqtt.Transport.WebSocket
```

## What it provides

| Type | Contract | Purpose |
| --- | --- | --- |
| `WebSocketTransportFactory` | `IMqttTransportFactory` | Creates a new WebSocket transport for each connection attempt. |
| `WebSocketTransportOptions` | Options | Endpoint, subprotocol, headers, proxy, and low-level client configuration. |
| `WebSocketTransport` | `IMqttTransport` | MQTT bytes over binary WebSocket frames. Usually created by the factory. |

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Transport.WebSocket;

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseTransportFactory(_ => new WebSocketTransportFactory(new WebSocketTransportOptions
    {
        Uri = new Uri("wss://broker.example.com/mqtt"),
    }));
```

The MQTT subprotocol defaults to `mqtt`.

## Configure directly

```csharp
var transportFactory = new WebSocketTransportFactory(new WebSocketTransportOptions
{
    Uri = new Uri("wss://broker.example.com/mqtt"),
});

await using var client = new ResilientMqttClient(
    transportFactory,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "device-worker" },
    });
```

## Reverse proxy and gateway options

```csharp
new WebSocketTransportOptions
{
    Uri = new Uri("wss://gateway.example.com/mqtt"),
    Headers = new Dictionary<string, string>
    {
        ["Authorization"] = $"Bearer {token}",
    },
    Proxy = proxy,
    ConfigureClient = options =>
    {
        options.RemoteCertificateValidationCallback = validationCallback;
        options.ClientCertificates.Add(clientCertificate);
    },
}
```

Use `Headers` for application or gateway headers. Use `ConfigureClient` for lower-level
`ClientWebSocketOptions` such as client certificates and certificate validation.

## Operational notes

- The factory creates a fresh WebSocket per reconnect attempt.
- MQTT packets are sent as binary frames.
- The resilient client still owns reconnect, subscriptions, offline queue flush, and health.
- Prefer `wss` for production traffic unless the connection is protected by another private
  network boundary.

## Related docs

- [Connecting with WebSocket](/guide/connecting#websocket)
- [Transport swap points](/guide/extending#transport)
- [Package add-ons](/guide/package-add-ons)
