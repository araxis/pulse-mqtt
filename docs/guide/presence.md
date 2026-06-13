# Presence

Device presence is two messages: a **birth** the client publishes the moment it is properly
online, and a **last will** the broker publishes on the client's behalf when the connection
dies ungracefully. Pulse makes both first-class, so the classic pattern needs zero application
code:

```
status/device-7   ← birth publishes "online" (retained) on every connection-up
status/device-7   ← the broker publishes the will "offline" (retained) on ungraceful loss
```

## The pattern in one block

```csharp
await using var client = await new PulseMqttClientBuilder()
    .WithTcp("broker.example.com")
    .WithClientId("device-7")
    .WithBirth("status/device-7", "online", MqttQualityOfService.AtLeastOnce, retain: true)
    .WithWill("status/device-7", "offline", MqttQualityOfService.AtLeastOnce, retain: true)
    .BuildAndStartAsync(ct);
```

Any subscriber of `status/device-7` now sees `online` whenever the device connects (including
every automatic reconnect), `offline` whenever it drops without a clean DISCONNECT, and —
because both messages are retained — the current state immediately upon subscribing.

## The last will

The will rides the CONNECT packet; the broker holds it and publishes it only on an ungraceful
end (a clean `StopAsync`/DISCONNECT withdraws it). Configure it three ways:

```csharp
// Fluent: text, bytes, or typed payloads, plus the full v5 message form.
.WithWill("status/device-7", "offline", retain: true)
.WithWill(new MqttWillMessage("status/device-7") { Payload = bytes, DelayInterval = 30 })
.WithWill("status/device-7", new DeviceStatus("offline"), retain: true)   // via the serializer

// Options (direct construction):
new ResilientMqttClientOptions { Will = new MqttWillMessage("status/device-7") { ... } }
```

With dependency injection it binds from configuration:

```json
{ "Mqtt": { "Devices": { "Will": {
    "Topic": "status/device-7", "Payload": "offline",
    "QualityOfService": "AtLeastOnce", "Retain": true, "DelaySeconds": 30 } } } }
```

The v5 **will delay** (`DelayInterval`/`DelaySeconds`) is worth knowing: the broker waits that
long before publishing the will, so a fast reconnect cancels the "offline" entirely — no
flapping on brief network blips.

## The will factory — fresh per connection

A static will is frozen at startup. A **factory** runs on *every connection attempt*, before
CONNECT is sent, so the will can carry timestamps, session counters, or current configuration:

```csharp
.WithWill(ct => ValueTask.FromResult(new MqttWillMessage("status/device-7")
{
    Payload = JsonSerializer.SerializeToUtf8Bytes(new { state = "offline", at = DateTimeOffset.UtcNow }),
    Retain = true,
}))
```

A throwing factory fails that connection attempt like any connect failure — classified by the
reconnect decision, never swallowed. With DI, register it via
`UseWillFactory(sp => token => ...)`.

## The birth message

Published automatically on every connection-up, at a deliberate point in the sequence:

1. The CONNECT/CONNACK handshake completes.
2. Re-subscription restores the durable subscription set.
3. **The birth publishes.**
4. The offline queue flushes.
5. The state becomes `Connected`.

Nobody ever observes "online" from a client whose session is not actually restored yet, and
the birth lands before any backlogged traffic.

```csharp
.WithBirth("status/device-7", "online", MqttQualityOfService.AtLeastOnce, retain: true)
.WithBirth(new MqttPublishPacket { Topic = ..., UserProperties = [...] })
.WithBirth("status/device-7", new DeviceStatus("online"), retain: true)   // typed, via the serializer
```

The **birth factory** mirrors the will factory and sees the connection attempt counter:

```csharp
.WithBirth((attempt, ct) => ValueTask.FromResult(new MqttPublishPacket
{
    Topic = "status/device-7",
    Payload = JsonSerializer.SerializeToUtf8Bytes(new { state = "online", attempt }),
    Retain = true,
}))
```

DI: bindable static form on `PulseMqttClientOptions.Birth`, factory via `UseBirthFactory`.

## When the birth fails

`BirthFailurePolicy` decides (an option, `WithBirthFailurePolicy(...)` fluently):

- **`FailConnection`** (default) — the connection-up fails and the reconnect cycle retries.
  Presence stays truthful: nobody observes a connected client whose announcement never went
  out.
- **`LogAndContinue`** — the failure is logged (`BirthPublishFailed`, event 7) and counted
  (`disposition="BirthFailed"` on the published-messages counter); the connection proceeds.

## Verified end to end

The integration suite proves the full cycle against a real Mosquitto with no application code:
connect → retained `online`; socket killed without a DISCONNECT → the broker publishes the
retained `offline` will; the automatic reconnect → `online` again.
