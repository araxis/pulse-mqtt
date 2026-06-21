# Errors

The exception types you can meet, and what each one means.

| Exception | Thrown when | Typical response |
| --- | --- | --- |
| `MqttException` | An operation could not complete: handshake timeout, the connection closed mid-operation, an acknowledgement or RPC reply timed out | Retry the operation or surface it; the supervisor handles the connection itself |
| `MqttProtocolException` | The peer violated the protocol: malformed packet, invalid QoS, unexpected packet type | Nothing to retry — the connection faults and reconnects; persistent occurrences mean a broken broker or middlebox |
| `MqttConnectRejectedException` | The broker answered CONNECT with a non-success reason; carries `ReasonCode`, plus `ReasonString` and `ServerReference` when the broker sent them | Inspect the reason: credentials, authorization, identifier, or redirect details |
| `TerminalMqttConnectException` | The reconnect machinery gave up: the decision classified a failure as final, or the attempt cap was reached | The client is `Faulted`; fix the cause, then `ConnectAsync` |
| `OfflineQueueFullException` | A publish hit a full offline queue under the `Reject` policy, or a `Block`ed publish exceeded `PublishWaitTimeout` | Shed load, raise capacity, or pick a drop policy |
| `InvalidOperationException` | API misuse: typed call without a serializer, `ConnectAsync` while running, registration validation failures | Fix the code or configuration; messages name the exact problem |

Guarantees worth knowing:

- The decoder throws **only** `MqttProtocolException` for malformed input — fuzz-verified, so
  hostile bytes cannot surface arbitrary exceptions.
- A route handler's exception never escapes its route: it is logged
  (`RouteHandlerFaulted`) and isolates that route only — see
  [Routing](../guide/routing#isolation-and-backpressure).
- A publish **never fails silently**: the outcome enumerates delivery, queueing, or the
  explicit drop — see [Publishing](../guide/publishing#outcomes--no-silent-loss).
