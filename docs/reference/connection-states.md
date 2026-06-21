# Connection states

The `ConnectionState` enum, as reported by `ResilientMqttClient.State` and streamed by
`WatchState`.

| State | Meaning | Leaves via |
| --- | --- | --- |
| `Disconnected` | Initial state, before the first connect | `ConnectAsync` |
| `Connecting` | First connection attempt in progress | success → `Connected`; failure → `WaitingRetry`; terminal → `Faulted` |
| `Connected` | A session is live | drop → `Reconnecting`; `DisconnectAsync` → `Stopped` |
| `Reconnecting` | The connection was lost; restoring | success → `Connected`; failure → `WaitingRetry` |
| `WaitingRetry` | Backing off between attempts | timer → `Connecting`/`Reconnecting` |
| `Faulted` | Terminal failure; **sticky** | explicit `ConnectAsync` |
| `Stopped` | Stopped at the caller's request | `ConnectAsync` |

Notes:

- `Connected` is only entered **after** the lifecycle hook ran (re-subscription) and the
  offline queue flushed — by the time you observe it, the session is fully restored.
- `Faulted` and drop transitions carry broker or connection details on
  `ConnectionStateChanged` — for example `SessionTakenOver` after a broker DISCONNECT, or
  `NotAuthorized` after a rejected CONNECT.
- Health checks map these as healthy (`Connected`), degraded (`Connecting`, `Reconnecting`,
  `WaitingRetry`), and unhealthy (the rest).

`ConnectionStateChanged` — the `WatchState` element:

| Field | Meaning |
| --- | --- |
| `Previous` / `Current` | The transition |
| `Attempt` | The connection attempt number, for retry telemetry |
| `Reason` | The broker's reason code, populated on drops and faults when one is known |
| `ReasonString` | MQTT 5 reason string text, when the broker supplied it |
| `ServerReference` | MQTT 5 server reference, useful for redirect-aware deployments |
| `Error` | The triggering exception for rejected connects, retry failures, disconnects, and terminal faults |

`ResilientMqttClient.GetDiagnosticsSnapshot()` gives the same state as a synchronous snapshot
for dashboards and support tools:

| Field | Meaning |
| --- | --- |
| `ClientId`, `State`, `Attempt`, `IsRunning`, `StateChangedAt` | Current lifecycle state |
| `LastReason`, `LastReasonString`, `LastServerReference`, `LastError` | Last known broker or fault detail |
| `OfflineQueueDepth`, `OfflineQueueDroppedCount` | Offline queue counters, or `null` if a custom store cannot report them |
| `SubscriptionCount`, `PendingSubscribeCount`, `PendingUnsubscribeCount` | Subscription bookkeeping |
| `BrokerCapabilities` | Current negotiated broker capabilities while connected; `null` in disconnected, retrying, stopped, or faulted states |
