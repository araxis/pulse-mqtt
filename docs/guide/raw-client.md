# The raw client

Underneath the resilient client sits a thinner layer for code that wants **one connection, no
supervision**: protocol bridges, conformance tooling, brokers, or resilience logic of your
own. It lives in `Pulse.Mqtt.Core`.

## Layering

```
ResilientMqttClient     supervision, routing, typed messaging, RPC, offline queue
        │
RawMqttClient           one session: handshake, keep-alive, QoS state machines
        │
MqttConnection          packet engine: framing, encode/decode, serialized sends
        │
IMqttTransport          bytes: TCP, TLS, WebSocket, in-memory, yours
```

Use the lowest layer that solves your problem — most applications never go below
`ResilientMqttClient`.

## RawMqttClient

One connection, explicit lifetime, no retries:

```csharp
var factory = new TcpTransportFactory(new TcpTransportOptions { Host = "broker" });
await using var client = new RawMqttClient(factory, new RawMqttClientOptions
{
    ConnAckTimeout = TimeSpan.FromSeconds(10),
});

MqttConnAckPacket connAck = await client.ConnectAsync(
    new MqttConnectPacket { ClientId = "bridge-1", KeepAliveSeconds = 30 }, ct);

if (connAck.ReasonCode != MqttReasonCode.Success)
{
    // The broker said no; the connection is already closed. Decide yourself.
}

MqttReasonCode result = await client.PublishAsync(
    new MqttPublishPacket { Topic = "a/b", Payload = bytes, QualityOfService = MqttQualityOfService.AtLeastOnce }, ct);

IReadOnlyList<MqttReasonCode> granted = await client.SubscribeAsync([new MqttTopicFilter("a/#")], ct);

await foreach (var message in client.Messages.ReadAllAsync(ct))
{
    // inbound QoS acknowledgements already handled
}

await client.DisconnectAsync(ct);
```

What it owns:

- The CONNECT/CONNACK handshake, with timeout.
- The keep-alive loop (PINGREQ on idle, faulting on a missed PINGRESP).
- Outbound QoS 1/2 state machines, packet-identifier allocation, acknowledgement matching —
  acknowledgements complete their waiters directly on the receive loop, with no queue hop.
- Inbound QoS handling and duplicate suppression, feeding the bounded `Messages` channel.

When the connection dies, `Messages` completes (with the error, if any) and in-flight
operations fail with `MqttException`. There is no reconnect — that is the resilient layer's
job.

## MqttConnection

The packet engine for protocol-level work — you see every packet:

```csharp
var transport = await factory.ConnectAsync(ct);
await using var connection = new MqttConnection(transport, new MqttConnectionOptions());
connection.Start();

await connection.SendAsync(new MqttConnectPacket { ClientId = "probe" }, ct);
MqttPacket first = await connection.Inbound.ReadAsync(ct);   // expect MqttConnAckPacket
```

Framing, decoding, a bounded inbound channel, and serialized sends — nothing else. The
in-process test broker is built on exactly this.

## The codec

Lowest level — spans in, spans out, no I/O:

```csharp
// Encode any packet, fixed header included:
MqttPacketWriter.Write(bufferWriter, packet);

// Frame and decode:
if (MqttFrameReader.TryReadFrame(buffer, out var header, out var body, out var consumed) == MqttFrameStatus.Complete)
{
    MqttPacket packet = MqttPacketDecoder.Decode(header, body, MqttProtocolVersion.V500);
}
```

All fifteen control packets, both protocol versions, fuzz-hardened: malformed input throws
`MqttProtocolException` and nothing else. Publish encoding without v5 properties allocates
zero bytes.
