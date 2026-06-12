# Options

Every options type, every property, every default.

## PulseMqttClientOptions (dependency injection)

Bindable from configuration; one instance per client name.

| Property | Default | Meaning |
| --- | --- | --- |
| `Host` | — (required) | Broker host name or IP |
| `Port` | `1883` | Broker port (conventionally 8883 for TLS) |
| `UseTls` | `false` | TLS for the default TCP transport |
| `ClientId` | — (required) | MQTT client identifier |
| `KeepAliveSeconds` | `60` | Keep-alive interval; `0` disables |
| `CleanStart` | `true` | Start a clean session |
| `Username` / `Password` | `null` | Broker credentials |
| `ProtocolVersion` | `V500` | `V500` or `V311` |
| `StartWithHost` | `true` | `false` hands start/stop to the application |

## ResilientMqttClientOptions

For direct construction; with DI these come from `PulseMqttClientOptions` plus builder swaps.

| Property | Default | Meaning |
| --- | --- | --- |
| `Connect` | — (required) | The CONNECT packet template used for every (re)connection |
| `Raw` | `new()` | Per-connection settings (below) |
| `OfflineQueue` | `new()` | Offline queue bounds and policy (below) |
| `Backoff` | `new()` | Default backoff bounds; ignored when `ReconnectStrategy` is set |
| `ReconnectStrategy` | backoff + jitter | The reconnect loop ([swap](../guide/extending#custom-reconnect-strategy)) |
| `ReconnectDecision` | auth-final | Retry-or-fault classification ([swap](../guide/extending#custom-reconnect-decision)) |
| `Lifecycle` | re-subscriber | Connection up/down hooks ([swap](../guide/extending#custom-lifecycle)) |
| `SessionStore` | in-memory | Durable subscription set ([swap](../guide/extending#custom-session-store)) |
| `MessageStore` | bounded in-memory | Offline publish queue ([swap](../guide/extending#custom-message-store)) |
| `Serializer` | `null` | Typed messaging; typed APIs throw until set |
| `Logger` | `null` (silent) | Structured log sink |

## RawMqttClientOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `Connection` | `new()` | Engine settings (below) |
| `ConnAckTimeout` | 30 s | Handshake wait for the CONNACK |
| `PingResponseTimeout` | 30 s | PINGRESP wait before faulting |
| `AcknowledgementTimeout` | 30 s | Publish/subscribe/unsubscribe acknowledgement wait |
| `InboundMessageCapacity` | `256` | Bound of the received-message queue |

## MqttConnectionOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `ProtocolVersion` | `V500` | Decode rules for inbound packets |
| `InboundQueueCapacity` | bounded | Capacity of the packet engine's inbound channel |
| `MaxInboundPacketSize` | bounded | Hard limit on a single inbound packet |

## OfflineQueueOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `Capacity` | `1024` | Maximum queued publishes |
| `Overflow` | `Block` | `Block` \| `DropOldest` \| `DropNewest` \| `Reject` |
| `IncludeQos0` | `false` | Queue QoS 0 publishes instead of dropping them |
| `PublishWaitTimeout` | `null` | `Block` wait bound before `OfflineQueueFullException`; `null` = indefinite |

## BackoffOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `BaseDelay` | 500 ms | First retry delay; doubles per attempt (with full jitter) |
| `MaxDelay` | 30 s | Cap on the exponential growth |
| `MaxAttempts` | `null` | Attempt limit; `null` retries indefinitely |

## MqttRouteOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `Capacity` | `64` | Bound of the route's queue |
| `Overflow` | `Wait` | `Wait` \| `DropOldest` \| `DropNewest` |
| `MaxConcurrency` | `1` | Concurrent handler invocations; 1 preserves order |
| `SubscriptionQualityOfService` | `AtLeastOnce` | QoS requested for the route's filter |

## MqttRequestOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `Timeout` | 30 s | Reply wait before `MqttException` |
| `QualityOfService` | `AtLeastOnce` | QoS of the request publish |

## TcpTransportOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `Host` / `Port` | — | Endpoint |
| `UseTls` | `false` | Wrap in TLS |
| `TlsTargetHost` | `null` | SNI override when it differs from `Host` |
| `ClientCertificates` | `null` | Mutual TLS |
| `ServerCertificateValidation` | platform default | Custom validation callback |

## WebSocketTransportOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `Uri` | — (required) | `ws://` or `wss://` endpoint |
| `SubProtocol` | `"mqtt"` | Negotiated subprotocol |
| `ConfigureClient` | `null` | Headers, proxy, certificates on the underlying client |
