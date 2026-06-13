# Connecting

A connection is described by two things: a **transport** (where the bytes go) and a **CONNECT
packet** (who you are to the broker). Everything else — retries, keep-alive, re-subscription —
is handled for you.

## TCP

The default. With dependency injection, `Host`, `Port`, and `UseTls` configure it:

```csharp
services.AddPulseMqttClient("devices", options =>
{
    options.Host = "broker.example.com";
    options.Port = 1883;
    options.ClientId = "my-service";
});
```

Directly, build the factory yourself:

```csharp
var factory = new TcpTransportFactory(new TcpTransportOptions
{
    Host = "broker.example.com",
    Port = 1883,
});
```

Sockets run with `NoDelay` and each packet is written as a single buffer — there is no
fragmentation penalty on proxied or Nagle-affected paths.

## TLS

```csharp
options.Port = 8883;
options.UseTls = true;
```

For client certificates, SNI overrides, or custom validation, configure the transport options:

```csharp
var factory = new TcpTransportFactory(new TcpTransportOptions
{
    Host = "broker.example.com",
    Port = 8883,
    UseTls = true,
    TlsTargetHost = "broker.internal",            // SNI, when it differs from Host
    ClientCertificates = certificates,            // mutual TLS
    ServerCertificateValidation = (s, c, ch, e) => /* pinning, custom roots */ true,
});
```

## WebSocket

From the `Pulse.Mqtt.Transport.WebSocket` package:

```csharp
.UseTransportFactory(_ => new WebSocketTransportFactory(new WebSocketTransportOptions
{
    Uri = new Uri("wss://broker.example.com/mqtt"),
    // SubProtocol defaults to "mqtt"; ConfigureClient customizes headers, proxy, certificates.
}))
```

## Credentials and identity

```csharp
options.ClientId = "my-service";        // required
options.Username = "device-42";
options.Password = "secret";
options.CleanStart = true;              // false resumes a broker-side session
```

Direct construction exposes the full CONNECT packet — will messages, session expiry, receive
maximum, user properties:

```csharp
var connect = new MqttConnectPacket
{
    ClientId = "my-service",
    KeepAliveSeconds = 30,
    CleanStart = false,
    Username = "device-42",
    Password = Encoding.UTF8.GetBytes("secret"),
};
```

The same packet template is used for every reconnection.

## Enhanced authentication (MQTT 5)

For challenge/response schemes — SCRAM, OAuth-style token exchanges, Kerberos — implement one
small interface and hand it to the client:

```csharp
public sealed class ScramAuthenticator : IMqttAuthenticator
{
    public string Method => "SCRAM-SHA-256";

    public ValueTask<ReadOnlyMemory<byte>?> NextDataAsync(
        ReadOnlyMemory<byte>? challenge, CancellationToken ct)
    {
        // null challenge → produce the initial data for CONNECT (or a re-auth start);
        // otherwise → answer the broker's challenge.
        return ValueTask.FromResult<ReadOnlyMemory<byte>?>(ComputeNextStep(challenge));
    }
}
```

```csharp
new ResilientMqttClientOptions
{
    Connect = connect,
    Raw = new RawMqttClientOptions { Authenticator = new ScramAuthenticator() },
}
```

The client carries the method and initial data on CONNECT, answers every broker AUTH challenge
through the authenticator until the broker concludes with a CONNACK, and supports
client-initiated **re-authentication** on a live connection:

```csharp
await client.ReAuthenticateAsync(token);   // e.g. when a token rotates
```

A throwing authenticator fails the attempt like any connect failure; with no authenticator
configured, no AUTH is ever sent and a broker that starts an exchange anyway is a protocol
error.

## Protocol version

MQTT 5.0 is the default. For brokers that only speak 3.1.1:

```csharp
options.ProtocolVersion = MqttProtocolVersion.V311;
```

The codec implements both completely; v5-only features (properties, response topics, shared
subscriptions) are simply absent on a 3.1.1 session.

## Keep-alive

`KeepAliveSeconds` (default 60, `0` disables) drives a PINGREQ loop when the connection is
idle. A missing PINGRESP within `RawMqttClientOptions.PingResponseTimeout` faults the
connection, which triggers the [reconnect cycle](./resilience). Brokers may override the
interval via the CONNACK's server keep-alive; Pulse honors it.

## Handshake timeouts

All on `ResilientMqttClientOptions.Raw`:

| Setting | Default | Meaning |
| --- | --- | --- |
| `ConnAckTimeout` | 30 s | How long to wait for the broker's CONNACK |
| `PingResponseTimeout` | 30 s | How long to wait for a PINGRESP |
| `AcknowledgementTimeout` | 30 s | How long to wait for publish/subscribe acknowledgements |
| `InboundMessageCapacity` | 256 | Bound of the received-message queue |

## Custom transports

Anything that moves bytes can carry MQTT: implement `IMqttTransportFactory` returning an
`IMqttTransport` (a `PipeReader`/`PipeWriter` pair). The in-process
[test broker](./testing) is exactly that — see [Extending](./extending#custom-transport).
