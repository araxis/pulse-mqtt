# Connection states

The `ConnectionState` enum, as reported by `ResilientMqttClient.State` and streamed by
`WatchState`.

| State | Meaning | Leaves via |
| --- | --- | --- |
| `Disconnected` | Initial state, before the first start | `StartAsync` |
| `Connecting` | First connection attempt in progress | success → `Connected`; failure → `WaitingRetry`; terminal → `Faulted` |
| `Connected` | A session is live | drop → `Reconnecting`; `StopAsync` → `Stopped` |
| `Reconnecting` | The connection was lost; restoring | success → `Connected`; failure → `WaitingRetry` |
| `WaitingRetry` | Backing off between attempts | timer → `Connecting`/`Reconnecting` |
| `Faulted` | Terminal failure; **sticky** | explicit `StartAsync` |
| `Stopped` | Stopped at the caller's request | `StartAsync` |

Notes:

- `Connected` is only entered **after** the lifecycle hook ran (re-subscription) and the
  offline queue flushed — by the time you observe it, the session is fully restored.
- `Faulted` carries the error on the `ConnectionStateChanged.Error` field of the transition.
- Health checks map these as healthy (`Connected`), degraded (`Connecting`, `Reconnecting`,
  `WaitingRetry`), and unhealthy (the rest).

`ConnectionStateChanged` — the `WatchState` element:

| Field | Meaning |
| --- | --- |
| `Previous` / `Current` | The transition |
| `Attempt` | The connection attempt number, for retry telemetry |
| `Error` | The triggering exception, when there is one |
