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
- `Faulted` and drop transitions carry the broker's reason code on the
  `ConnectionStateChanged.Reason` field — for example `SessionTakenOver` after a broker
  DISCONNECT, or `NotAuthorized` after a rejected CONNECT.
- Health checks map these as healthy (`Connected`), degraded (`Connecting`, `Reconnecting`,
  `WaitingRetry`), and unhealthy (the rest).

`ConnectionStateChanged` — the `WatchState` element:

| Field | Meaning |
| --- | --- |
| `Previous` / `Current` | The transition |
| `Attempt` | The connection attempt number, for retry telemetry |
| `Reason` | The broker's reason code, populated on drops and faults when one is known |
