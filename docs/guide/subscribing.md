# Subscribing

Subscribing is the broker-delivery contract: it tells the MQTT broker which filters to send to
this client. **[Routing](./routing)** is the local-dispatch contract layered on top: templates,
handlers, and streams for messages the broker already delivers.

## Subscribe and unsubscribe

```csharp
IReadOnlyList<MqttReasonCode> granted = await client.SubscribeAsync(
    [new MqttTopicFilter("sensors/+/temp") { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
    token);

await client.UnsubscribeAsync(["sensors/+/temp"], token);
```

For route-template subscriptions, pass the parsed template directly when you do not need MQTT
5 subscription flags:

```csharp
await client.SubscribeAsync(
    MqttRouteTemplate.Parse("sensors/{device}/temp"),
    MqttQualityOfService.AtLeastOnce,
    token);
```

The returned reason codes are the broker's per-filter grants (`GrantedQualityOfService1`,
`NotAuthorized`, …) when a connection is live. Offline, the call still succeeds: the filters
join the **durable subscription set** and are applied on the next connection — the result list
is then empty.

## The durable subscription set

Every subscribe/unsubscribe updates a client-side set persisted through the
[`ISessionStore`](./extending#custom-session-store). On every reconnect that lost the broker
session, the default lifecycle **re-subscribes the whole set before the offline queue
flushes** — so a flushed publish can never arrive before its subscriber is back.

Updates are incremental (no full-set rewrite per call): subscribing 10,000 topics one at a
time costs ~19 MB total, not gigabytes.

## Topic filters

Standard MQTT semantics, validated and matched exactly per specification:

| Filter | Matches | Does not match |
| --- | --- | --- |
| `sport/tennis/+` | `sport/tennis/player1` | `sport/tennis/player1/ranking` |
| `sport/#` | `sport`, `sport/tennis/player1` | `sports` |
| `+/+` | `/finance` | `finance` |

Wildcard filters never match `$`-prefixed system topics, and `#` matches its parent level.

## Shared subscriptions (MQTT 5)

For load-balanced consumer groups, build the `$share` filter with the helper:

```csharp
var filter = MqttSharedSubscription.Format("workers", "jobs/+/created");
// "$share/workers/jobs/+/created"
await client.SubscribeAsync([new MqttTopicFilter(filter)], token);
```

The broker delivers each matching message to **one** member of the group.

## MQTT 5 subscription options

`MqttTopicFilter.NoLocal`, `RetainAsPublished`, and `RetainHandling`, plus
`MqttSubscribePacket.SubscriptionIdentifier`, are MQTT 5-only. Pulse rejects those options when a
SUBSCRIBE packet is encoded as `MqttProtocolVersion.V311`; MQTT 3.1.1 can only carry the topic
filter and maximum QoS. See the
[MQTT protocol compatibility matrix](../reference/protocol-compatibility).

## Consuming the raw message stream

Below routing sits a single bounded channel of everything the client receives:

```csharp
await foreach (MqttPublishPacket message in client.Messages.ReadAllAsync(token))
{
    Process(message);
}
```

The channel is bounded (`RawMqttClientOptions.InboundMessageCapacity`, default 256); a slow
consumer slows the socket reader rather than growing memory. The channel survives reconnects —
one continuous stream across sessions.

For pipeline-style consumers, `Pulse.Mqtt.Dataflow` exposes the same raw stream as a bounded
`ISourceBlock<MqttPublishPacket>`:

```csharp
await using var source = client.ToMessageSourceBlock(
    new MqttDataflowSourceOptions { BoundedCapacity = 128 },
    token);
```

::: warning Pick one consumer model
Use either the raw `Messages` stream or [routing](./routing), not both: the router consumes
from the same stream. Routing is the right default; the raw stream suits gateway-style code
that forwards everything.
:::

## Inbound QoS handling

Acknowledgements for received messages are automatic: QoS 1 messages are acknowledged after
they are accepted into the inbound queue; QoS 2 messages run the full
PUBREC/PUBREL/PUBCOMP exchange with duplicate suppression. Your code only ever sees each
message once.

When broker acknowledgement must wait for application work, use a manual route delivery mode:
`client.Route(...).ManualAcknowledgement()`, `RegisterManualAcknowledgementRoute(...)`, or
[`OpenAcknowledgedRouteStream`](./routing#delivery-modes). Ordinary `Messages`,
`OpenRouteStream`, and automatic routes stay automatic.
Negative acknowledgement is protocol-version dependent: MQTT 5 QoS 1/2 deliveries expose
`CanReject = true`; MQTT 3.1.1 and QoS 0 cannot carry a per-message rejection.
