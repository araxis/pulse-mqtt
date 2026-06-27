# MQTT protocol compatibility

Pulse MQTT supports MQTT 5.0 and MQTT 3.1.1. MQTT 5.0 is the default. MQTT 3.1.1 is supported for
brokers that do not speak MQTT 5, but it has no packet properties, no AUTH packet, no response
topic/correlation request-response convention, and fewer acknowledgement details.

The library owns the protocol boundary:

- MQTT 5-only helper APIs fail fast when the client is configured for `MqttProtocolVersion.V311`.
- Packet codecs reject MQTT 5-only packet properties when `ProtocolVersion` is `V311`, instead of
  silently dropping them.
- `Pulse.Mqtt.Analyzers` reports `PMQ0004` for explicit MQTT 3.1.1 packet initializers that set
  known MQTT 5-only properties.
- `ResilientMqttClient.GetBrokerCapabilitiesSnapshot()` exposes negotiated protocol and broker
  feature support after a successful connection.

## Feature matrix

| Feature | MQTT 5.0 | MQTT 3.1.1 | Library behavior |
| --- | --- | --- | --- |
| Basic CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, DISCONNECT | Supported | Supported | Same public client APIs. |
| QoS 0, QoS 1, QoS 2 publish flows | Supported | Supported | Same public client APIs. |
| Retained messages | Supported | Supported | Same publish flag and subscribe behavior. |
| Username and password credentials | Supported | Supported | Same `MqttConnectPacket` fields. |
| Clean session / clean start | Supported | Supported | `CleanStart` maps to the protocol's session-start flag. |
| Session expiry interval | Supported | Not supported | `MqttConnectPacket.SessionExpiryInterval` is rejected on `V311`. |
| Receive maximum | Supported | Not supported | `MqttConnectPacket.ReceiveMaximum` and CONNACK receive maximum are rejected on `V311`. |
| Maximum packet size negotiation | Supported | Not supported | MQTT 5 CONNACK limit is enforced when negotiated; packet-size properties are rejected on `V311`. |
| Topic aliases | Supported | Not supported | Alias properties are rejected on `V311`; capability snapshot reports topic aliases as not supported. |
| Payload format, content type, message expiry | Supported | Not supported | Publish/will properties are rejected on `V311`; encode metadata in the payload for MQTT 3.1.1. |
| User properties | Supported | Not supported | Packet user properties are rejected on `V311`; use payload envelopes for MQTT 3.1.1 metadata. |
| Response topic and correlation data | Supported | Not supported | Request/response helpers throw `NotSupportedException` on `V311`. |
| Enhanced authentication and re-authentication | Supported | Not supported | `RawMqttClientOptions.Authenticator` and `ReAuthenticateAsync` throw on `V311`. |
| Reason strings and server references | Supported | Not supported | DISCONNECT, CONNACK, and ack reason metadata is rejected on `V311`. |
| Negative acknowledgement reason codes | Supported for protocol paths that can carry them | Not supported | Inbound publish rejection exposes `CanReject = false` on MQTT 3.1.1. |
| Subscription identifiers | Supported | Not supported | SUBSCRIBE subscription identifiers are rejected on `V311`; capability snapshot reports not supported. |
| `NoLocal`, `RetainAsPublished`, `RetainHandling` subscription options | Supported | Not supported | `MqttTopicFilter` options are rejected when encoded in a `V311` SUBSCRIBE packet. |
| Shared subscriptions | Standardized | Broker-specific extension at best | Documented as MQTT 5 behavior; check broker support before relying on it. |
| Trace context in user properties | Supported | Not supported | `PropagateTraceContext` uses user properties only on MQTT 5; use trace envelopes for MQTT 3.1.1. |
| Broker capabilities snapshot | Supported | Supported with unknown/not-supported markers | MQTT 5 CONNACK values are populated when negotiated; MQTT 3.1.1 reports non-negotiated optional support as `Unknown` and MQTT 5-only features as `NotSupported`. |

## Runtime guardrails

Packet objects keep one shape so advanced users can work close to the wire. The codec enforces
the negotiated packet version when writing bytes:

```csharp
var packet = new MqttPublishPacket
{
    Topic = "orders/created",
    ProtocolVersion = MqttProtocolVersion.V311,
    ContentType = "application/json", // throws during encode/send
};
```

Use MQTT 5.0 when metadata needs protocol-level properties:

```csharp
var packet = new MqttPublishPacket
{
    Topic = "orders/created",
    ProtocolVersion = MqttProtocolVersion.V500,
    ContentType = "application/json",
};
```

For MQTT 3.1.1 deployments, put metadata in the payload envelope or in a documented topic
convention. The observability guide shows the same pattern for trace context.

## Capability checks

After a successful connection, inspect broker-negotiated support before enabling optional MQTT 5
behavior:

```csharp
var capabilities = client.GetBrokerCapabilitiesSnapshot();

if (capabilities?.TopicAliases == MqttBrokerFeatureSupport.Supported)
{
    logger.LogInformation(
        "Broker supports {AliasCount} topic aliases",
        capabilities.EffectiveTopicAliasMaximum);
}
```

The snapshot is `null` until connected and is cleared when the client leaves `Connected`.
