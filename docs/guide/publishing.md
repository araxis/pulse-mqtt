# Publishing

## The basic shapes

Typed, through the configured [serializer](./typed-messaging):

```csharp
var outcome = await client.PublishAsync(
    "sensors/boiler-1/telemetry",
    new TelemetryReading("C", 21.5, DateTimeOffset.UtcNow),
    MqttQualityOfService.AtLeastOnce,
    retain: false,
    cancellationToken: token);
```

Raw, with full control over the packet:

```csharp
var outcome = await client.PublishAsync(new MqttPublishPacket
{
    Topic = "sensors/boiler-1/telemetry",
    Payload = payloadBytes,
    QualityOfService = MqttQualityOfService.AtLeastOnce,
    Retain = false,
}, token);
```

## Quality of service

| Level | Wire exchange | The returned task completes when |
| --- | --- | --- |
| `AtMostOnce` (0) | PUBLISH | the packet is flushed to the transport |
| `AtLeastOnce` (1) | PUBLISH → PUBACK | the broker acknowledged |
| `ExactlyOnce` (2) | PUBLISH → PUBREC → PUBREL → PUBCOMP | the full exchange completed |

Packet identifiers are assigned by the client — never set `PacketIdentifier` yourself. QoS 1
and 2 publishes are awaited to their acknowledgement with `AcknowledgementTimeout` as the
upper bound.

## Outcomes — no silent loss

Every publish returns a `PublishOutcome`:

| Disposition | Meaning |
| --- | --- |
| `Delivered` | The broker received it (QoS > 0: acknowledged). `ReasonCode` carries the broker's answer. |
| `Queued` | The client is offline; the message sits in the [offline queue](./resilience#the-offline-queue) and flushes after reconnect, **after** re-subscription. |
| `DroppedOffline` | The client is offline and QoS 0 messages are configured to drop (the default for QoS 0). |

```csharp
var outcome = await client.PublishAsync(packet, token);
if (outcome.Disposition == PublishDisposition.Queued)
{
    logger.LogWarning("Broker unreachable; message queued");
}
```

## Retained messages

```csharp
new MqttPublishPacket { Topic = "devices/boiler-1/config", Payload = bytes, Retain = true }
```

The broker stores the last retained message per topic and hands it to new subscribers
immediately.

## MQTT 5 properties

The publish packet exposes the full v5 property set:

```csharp
new MqttPublishPacket
{
    Topic = "orders/created",
    Payload = bytes,
    ContentType = "application/json",
    PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8,
    MessageExpiryInterval = 3600,                        // seconds
    ResponseTopic = "orders/created/ack",                // see request/response
    CorrelationData = correlationBytes,
    UserProperties = [new MqttUserProperty("tenant", "acme")],
}
```

[Typed publishes](./typed-messaging) stamp `ContentType` and `PayloadFormatIndicator` from the
serializer automatically.

## Publishing while offline

Nothing special to write — publish as usual and read the outcome. QoS 1/2 messages are queued
(bounded, with your chosen overflow policy) and flushed in order after the next successful
reconnect; QoS 0 messages drop by default or queue with `IncludeQos0 = true`. Details and
tuning live in [Resilience](./resilience#the-offline-queue).

## Concurrency

`PublishAsync` is safe to call from any number of tasks concurrently. Sends are serialized on
the connection in call order; QoS 1/2 acknowledgements complete out of order as the broker
answers, so a slow acknowledgement never blocks the pipe. The in-flight window is bounded by
packet-identifier availability (65,535) and the broker's receive maximum.

## Performance notes

- A publish without v5 properties encodes in a **single pass with zero allocation** — the
  payload is copied exactly once, into the transport buffer.
- Each packet is one TCP write: no fragmentation, no Nagle interaction on proxied paths.
- See [Performance](./performance) for measured numbers.
