# QUIC transport package

Package: `Pulse.Mqtt.Transport.Quic`

Use this package to reach a broker over MQTT-over-QUIC: one TLS 1.3 handshake, no TCP
head-of-line blocking, and connection migration built into the protocol. EMQX (port 14567) and
NanoMQ support it; MQTT-over-QUIC is broker-specific and not yet an OASIS standard, so check
your broker before reaching for it.

## Install

```shell
dotnet add package Pulse.Mqtt.Transport.Quic
```

The package targets .NET 10 only, because `System.Net.Quic` is stable from .NET 9. It also
needs the msquic native library at runtime:

| Platform | Requirement |
| --- | --- |
| Windows | Windows 11 / Server 2022 or later (msquic ships with the .NET runtime) |
| Linux | `libmsquic` installed (`apt-get install libmsquic` from the Microsoft package repository) |
| macOS | Supported by .NET when msquic is available |

Check `QuicTransportFactory.IsSupported` at startup and fall back to TCP or WebSocket when it
returns `false`.

## What it provides

| Type | Contract | Purpose |
| --- | --- | --- |
| `QuicTransportFactory` | `IMqttTransportFactory` | Creates a new QUIC connection and stream for each connection attempt. |
| `QuicTransportOptions` | Options | Endpoint, ALPN, TLS validation, client certificates, and low-level connection configuration. |
| `QuicTransport` | `IMqttTransport` | MQTT bytes over a single bidirectional QUIC stream. Usually created by the factory. |

## Configure with dependency injection

```csharp
using Pulse.Mqtt.Transport;

builder.Services
    .AddPulseMqttClient("devices", configure)
    .UseTransportFactory(_ => new QuicTransportFactory(new QuicTransportOptions
    {
        Host = "broker.example.com",
    }));
```

The port defaults to 14567 (the EMQX convention) and the ALPN protocol to `mqtt`.

## Configure directly

```csharp
var transportFactory = new QuicTransportFactory(new QuicTransportOptions
{
    Host = "broker.example.com",
});

await using var client = new ResilientMqttClient(
    transportFactory,
    new ResilientMqttClientOptions
    {
        Connect = new MqttConnectPacket { ClientId = "device-worker" },
    });
```

## TLS options

QUIC always negotiates TLS 1.3; there is no plaintext mode.

```csharp
new QuicTransportOptions
{
    Host = "broker.example.com",
    TlsTargetHost = "public-name.example.com",
    ClientCertificates = clientCertificates,
    ServerCertificateValidation = validationCallback,
    ConfigureConnection = options =>
    {
        options.MaxInboundUnidirectionalStreams = 0;
        options.IdleTimeout = TimeSpan.FromMinutes(2);
    },
}
```

Use `ServerCertificateValidation` only to integrate a custom trust decision — returning
`true` unconditionally disables validation and belongs in tests. Use `ConfigureConnection`
for lower-level `QuicClientConnectionOptions` the structured options do not cover.

## Fallback when QUIC is unavailable

```csharp
IMqttTransportFactory transportFactory = QuicTransportFactory.IsSupported
    ? new QuicTransportFactory(new QuicTransportOptions { Host = host })
    : new TcpTransportFactory(new TcpTransportOptions { Host = host, Port = 8883, UseTls = true });
```

`ConnectAsync` on an unsupported platform throws `PlatformNotSupportedException` with the same
guidance.

## Operational notes

- The factory creates a fresh QUIC connection and one bidirectional stream per reconnect
  attempt; the resilient client still owns reconnect, subscriptions, offline queue flush, and
  health.
- The conformance suite runs against EMQX's QUIC listener in CI, alongside the TCP matrix.
- Multi-stream mode (one QUIC stream per topic, an EMQX extension) is not implemented; the
  transport uses the single-stream mode every MQTT-over-QUIC broker supports.

## Related docs

- [Broker compatibility](/reference/broker-compatibility)
- [Transport swap points](/guide/extending#transport)
- [Package add-ons](/guide/package-add-ons)
