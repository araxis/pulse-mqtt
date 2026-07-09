# Pulse.Mqtt.Transport.Quic

MQTT over QUIC transport for Pulse MQTT clients. Use it only when the broker supports MQTT-over-QUIC and the runtime platform supports `System.Net.Quic`.

## Install

```shell
dotnet add package Pulse.Mqtt.Transport.Quic
```

This package targets .NET 10 only. It also needs the native msquic library at runtime. Check support before selecting the transport in production.

```csharp
if (!QuicTransportFactory.IsSupported)
{
    throw new PlatformNotSupportedException("QUIC transport is not available on this host.");
}
```

## Configure with dependency injection

```csharp
builder.Services
    .AddPulseMqttClient("telemetry", configure)
    .UseTransportFactory(_ => new QuicTransportFactory(
        new QuicTransportOptions
        {
            Host = "broker.example.com",
            Port = 14567,
        }));
```

## Configure directly

```csharp
var transportFactory = new QuicTransportFactory(new QuicTransportOptions
{
    Host = "broker.example.com",
    TlsTargetHost = "broker.example.com",
});

await using var client = new ResilientMqttClient(
    transportFactory,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "telemetry-worker" },
    });
```

QUIC always uses TLS 1.3. The resilient client still owns reconnect, subscriptions, offline queue flush, and health.

Full docs: https://araxis.github.io/pulse-mqtt/packages/transport-quic
