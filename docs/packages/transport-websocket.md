# WebSocket transport package

Package: `Pulse.Mqtt.Transport.WebSocket`

Use this package when the broker is exposed through `ws` or `wss`.

## Install

```shell
dotnet add package Pulse.Mqtt.Transport.WebSocket
```

## Configure

```csharp
.UseTransportFactory(_ => new WebSocketTransportFactory(new WebSocketTransportOptions
{
    Uri = new Uri("wss://broker.example.com/mqtt"),
}))
```

The subprotocol defaults to `mqtt`.

## Reverse proxy options

```csharp
new WebSocketTransportOptions
{
    Uri = new Uri("wss://gateway.example.com/mqtt"),
    Headers = new Dictionary<string, string>
    {
        ["Authorization"] = $"Bearer {token}",
    },
    Proxy = proxy,
}
```

Use `ConfigureClient` for lower-level socket options such as client certificates.

See [Connecting](/guide/connecting#websocket) for the full transport guide.
