# Phase 4 — Resilience: Detailed Design

> Deep-dive companion to [NG-MQTT-Client-Development-Plan.md](NG-MQTT-Client-Development-Plan.md) (Part E, Phase 4).
> **Why this phase is the flagship:** verified research (§A.0) shows MQTTnet v5 *removed* `ManagedMqttClient` and has no replacement planned — the market leader ships no first-class reconnect / offline queue / auto-resubscribe. This is Pulse's single clearest opening. Everything here is **swappable** (Part D of the plan): the user can replace auto-connect with a Polly pipeline, the offline store with SQLite, or disable resilience entirely — without forking.

---

## 4.0 Scope (Implements)

> After Phase 4, Pulse has **one always-resilient client**: it auto-connects, survives broker drops with bounded backoff, restores session state and re-subscribes, queues outbound publishes while offline (bounded, with an explicit overflow policy), flushes them in order on reconnect, emits an observable connection-state stream and lifecycle callbacks, and **stops cleanly on terminal auth failures instead of looping forever** (the documented MQTTnet pain). The default backoff strategy and a Polly strategy both satisfy the *same* acceptance tests.

**Out of scope here (owned by other phases):** the raw connection engine + QoS state machines (Phase 3), topic routing/typed handlers (Phase 5), full inbound flow-control/Receive-Maximum tuning (Phase 9 — Phase 4 only honors the negotiated window as a hard cap), durable store *implementations* (Phase-4 ships the in-memory default; `Sqlite`/`LiteDb` are Phase-4-compatible add-ons delivered later).

---

## 4.1 Component model

```
                       ResilientMqttClient  (the only client users hold)
                       ─────────────────────────────────────────────
  user ── Publish/Subscribe ─►│  outbound: OfflineOutboundQueue        │
                              │            (bounded Channel<T> + IMessageStore)
  user ◄─ ConnectionState ────│  state machine + lifecycle events       │
                              │                                         │
                              │  Supervisor loop ──► IReconnectStrategy │  ◄── swap: Backoff | Polly | None
                              │        │                                │
                              │        ├─ connectOnce ─► RawMqttClient   │  (Phase 3)
                              │        ├─ IReconnectDecision (reason codes)
                              │        ├─ IConnectionLifecycle.OnUp/OnDown
                              │        └─ ISessionStore (resubscribe/restore)
                              └─────────────────────────────────────────
```

`ResilientMqttClient` **owns** the lifecycle and **delegates**: connecting-with-retry to `IReconnectStrategy`, retry-or-quit decisions to `IReconnectDecision`, post-connect work to `IConnectionLifecycle`, durable state to `ISessionStore`/`IMessageStore`. Each delegate is a swap point.

---

## 4.2 The connection state machine

States are observable (`ConnectionState`), transitions are the only way state changes, and every transition raises an event + a metric + a log scope.

```
            start
              │
              ▼
        ┌───────────┐  connectOnce succeeds   ┌───────────┐
        │Connecting │ ───────────────────────►│ Connected │
        └───────────┘                          └───────────┘
           ▲   │ transient failure                 │  broker drop / network loss
           │   │ (IReconnectDecision: Retry)       ▼
           │   ▼                              ┌────────────┐
        ┌──────────────┐  delay elapsed       │ Reconnecting│
        │ WaitingRetry │◄─────────────────────└────────────┘
        └──────────────┘
              │ terminal failure (IReconnectDecision: Stop)
              ▼
        ┌──────────┐         StopAsync() from any state      ┌──────────┐
        │ Faulted  │                                          │ Stopped  │
        └──────────┘                                          └──────────┘
```

```csharp
public enum ConnectionState
{
    Disconnected,   // initial / after StopAsync
    Connecting,     // first attempt in progress
    Connected,      // session live
    Reconnecting,   // lost connection, attempting to restore
    WaitingRetry,   // backing off between attempts
    Faulted,        // terminal: gave up (e.g. NotAuthorized) — requires explicit restart
    Stopped         // user-requested graceful stop
}
```

**Rules**
- `Connecting` vs `Reconnecting` are distinct so consumers/metrics can tell a first connect from a recovery.
- `Faulted` is terminal and **sticky**: the supervisor does not auto-retry out of it. The user must call `StartAsync` again (typically after fixing credentials). This is the explicit fix for MQTTnet's "auth failure → reconnect loop spins forever / `InvalidOperationException`" reports.
- Transitions are serialized through a single owning loop (no locks on the hot path; the supervisor is the sole writer of state).

```csharp
public readonly record struct ConnectionStateChanged(
    ConnectionState Previous,
    ConnectionState Current,
    MqttDisconnectReason? Reason,   // populated on drops/faults
    int Attempt);                   // reconnect attempt counter (0 on first connect)
```

Exposed two ways (both swap-free, idiomatic):
```csharp
ConnectionState State { get; }                              // current value
IAsyncEnumerable<ConnectionStateChanged> WatchState(CancellationToken ct);  // stream
event Action<ConnectionStateChanged>? StateChanged;         // event sink
```

---

## 4.3 Swap point #1 — `IReconnectStrategy` (owns the loop)

The strategy owns the **retry loop**, so it can be fully replaced (default backoff ↔ Polly ↔ none) without the supervisor knowing how retry happens. Contract (frozen in `Pulse.Mqtt.Core.Abstractions`):

```csharp
/// One connect attempt. Throws TransientMqttConnectException on retryable failure,
/// TerminalMqttConnectException on non-retryable (e.g. NotAuthorized).
public delegate Task ConnectOnceAsync(CancellationToken ct);

public interface IReconnectStrategy
{
    /// Drives (re)connection: invokes connectOnce repeatedly per its own policy
    /// until success (returns), a terminal failure (rethrows), or cancellation.
    /// Reports each attempt via context (for state/metrics/logging).
    Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken ct);
}

public interface IReconnectContext
{
    int Attempt { get; }                          // 1-based, current attempt
    void OnAttemptStarting(int attempt);          // supervisor → WaitingRetry/Connecting transitions
    void OnAttemptFailed(int attempt, Exception error);
    TimeProvider Time { get; }                    // ALL delays go through this (testable)
}
```

### Default — `BackoffReconnectStrategy` (in Core, no dependencies)
Exponential backoff + full jitter, capped, infinite attempts by default, all delays via `TimeProvider`. Consults `IReconnectDecision` (§4.4) to decide retry-vs-stop; rethrows `TerminalMqttConnectException` so the supervisor faults.

```csharp
public sealed class BackoffReconnectStrategy(BackoffOptions options, IReconnectDecision decision)
    : IReconnectStrategy
{
    public async Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext ctx, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            ctx.OnAttemptStarting(attempt);
            try { await connectOnce(ct); return; }                    // success
            catch (TerminalMqttConnectException) { throw; }           // → supervisor faults
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                ctx.OnAttemptFailed(attempt, ex);
                if (!decision.ShouldRetry(attempt, ex)) throw;        // give up → faulted
                var delay = Backoff.Compute(attempt, options);        // exp + full jitter, capped
                await Task.Delay(delay, ctx.Time, ct);                // testable via FakeTimeProvider
            }
        }
    }
}
```

### Swap — `PollyReconnectStrategy` (in `Pulse.Mqtt.Resilience.Polly`)
The user owns the entire policy via a Polly v8 `ResiliencePipeline`. Terminal exceptions are simply not handled by the pipeline, so they escape and fault the supervisor — same semantics, different engine.

```csharp
public sealed class PollyReconnectStrategy(ResiliencePipeline pipeline) : IReconnectStrategy
{
    public Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext ctx, CancellationToken ct) =>
        pipeline.ExecuteAsync(async token => await connectOnce(token), ct).AsTask();
}
```

Registration — the *only* change the user makes:
```csharp
b.UseReconnectStrategy(_ => new PollyReconnectStrategy(
    new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            MaxRetryAttempts = int.MaxValue,
            ShouldHandle = new PredicateBuilder().Handle<TransientMqttConnectException>()
        })
        .Build()));
```

### Swap — `NoReconnectStrategy`
`RunAsync` calls `connectOnce` exactly once; on failure it rethrows. Use when an outer system owns lifecycle. `b.UseReconnectStrategy(new NoReconnectStrategy());`

> **Inspiration (lead, unverified — §research):** Go `autopaho` proves the "retry until up, callbacks on up/down" model; rumqtt proves explicit-capacity backpressure. We adopt both shapes behind these contracts.

---

## 4.4 Swap point #2 — `IReconnectDecision` (CONNACK reason-code classification)

The fix for "auth failure loops forever." After a failed connect or a server `DISCONNECT`, classify the reason: **transient** (retry) vs **terminal** (fault and stop). Pluggable, with a sensible default.

```csharp
public interface IReconnectDecision
{
    bool ShouldRetry(int attempt, Exception error);
}
```

Default classification of MQTT 5 CONNACK / DISCONNECT reason codes:

| Reason code | Class | Default |
|---|---|---|
| `0x00` Success | — | connect proceeds |
| `0x87` NotAuthorized, `0x86` BadUserNameOrPassword, `0x85` ClientIdNotValid, `0x8C` BadAuthMethod, `0x9A` Banned | **Terminal** | **Stop → Faulted** |
| `0x88` ServerUnavailable, `0x89` ServerBusy, `0x97` QuotaExceeded, `0x82` ProtocolError(transient), connection-refused / socket / TLS-transient | **Transient** | Retry with backoff |
| `0x8F` TopicFilterInvalid, `0x95` PacketTooLarge | **Terminal** (config bug) | Stop → Faulted |
| Network timeout / `IOException` / `SocketException` | **Transient** | Retry |

Override examples:
- *"Treat NotAuthorized as transient because my token refreshes out-of-band"*: `b.UseReconnectDecision(new RetryAuthDecision())` (and pair with an `IMqttCredentialsProvider` that refreshes — Phase 8/Part C.8).
- *"Stop after N attempts regardless"*: `b.UseReconnectDecision(new MaxAttemptsDecision(50))`.

`connectOnce` (built by the supervisor) maps the raw CONNACK to the right exception type using the decision, so the strategy stays policy-agnostic:
```csharp
var connack = await raw.ConnectAsync(opts, ct);
if (connack.ReasonCode != MqttConnectReasonCode.Success)
    throw decision.ShouldRetry(attempt, MqttConnectException.From(connack))
        ? new TransientMqttConnectException(connack)
        : new TerminalMqttConnectException(connack);
```

---

## 4.5 Swap point #3 — `IConnectionLifecycle` (autopaho-style up/down callbacks)

After every successful (re)connect, before the connection is announced "ready," the supervisor runs the lifecycle hook. This is where **re-subscription** and app-level priming happen. Modeled on `autopaho`'s `OnConnectionUp`/`OnConnectionDown` (research lead).

```csharp
public interface IConnectionLifecycle
{
    /// Runs after CONNACK, before queue flush and before the client is marked Connected
    /// to callers. Use to re-subscribe. session.Present tells you whether the broker
    /// retained your subscriptions (skip resubscribe) or not (resubscribe needed).
    ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken ct);

    ValueTask OnConnectionDownAsync(MqttDisconnectReason reason, CancellationToken ct);
}

public interface IConnectionUpContext
{
    MqttConnAckInfo ConnAck { get; }            // includes SessionPresent + negotiated caps
    ISubscriptionRegistrar Subscriptions { get; } // resubscribe helper (idempotent)
    int Attempt { get; }
}
```

**Default `IConnectionLifecycle`** (in Client): when `SessionPresent == false`, replays the durable subscription set from `ISessionStore`; otherwise no-op (the broker kept them). Idempotent: re-running is safe. Users compose their own to subscribe to extra topics or warm caches.

> Ordering guarantee: **resubscribe completes before the offline queue flushes** (you don't want to publish a request whose reply topic you haven't re-subscribed to yet).

---

## 4.6 Swap point #4 — Offline outbound queue (`IMessageStore` + bounded `Channel<T>`)

Outbound publishes issued while not `Connected` are enqueued, then flushed in order on connect. Two layers:

1. **`IMessageStore`** — durability boundary (swap point). Default `InMemoryMessageStore` (bounded ring); swap to `SqliteMessageStore`/`LiteDbMessageStore` for crash-durable queues.
2. **`OfflineOutboundQueue`** — a small owned object wrapping a **bounded `System.Threading.Channels.Channel<QueuedPublish>`** with an explicit capacity and overflow policy (rumqtt-style explicit capacity → natural backpressure).

```csharp
public interface IMessageStore
{
    ValueTask EnqueueAsync(QueuedPublish msg, CancellationToken ct);   // honors capacity/overflow
    IAsyncEnumerable<QueuedPublish> DrainAsync(CancellationToken ct);  // FIFO, for flush
    ValueTask AcknowledgeAsync(ulong sequence, CancellationToken ct);  // remove after broker ack
    int Count { get; }
}

public sealed record OfflineQueueOptions
{
    public int Capacity { get; init; } = 1024;
    public OverflowPolicy Overflow { get; init; } = OverflowPolicy.Block;  // Block | DropOldest | DropNewest | Reject
    public bool IncludeQos0 { get; init; } = false;   // QoS0 is best-effort; drop while offline by default
    public TimeSpan? PublishWaitTimeout { get; init; } // for Block: how long Publish awaits capacity
}
```

**Behavior**
- **QoS 1/2** offline publishes are queued and **durable** (via `IMessageStore`); acknowledged-and-removed only after the broker confirms (PUBACK/PUBCOMP — Phase 3 state machine). Survives process restart when a durable store is used.
- **QoS 0** offline publishes follow `IncludeQos0` (default: drop, log a `messages.dropped` metric — never silently).
- **Overflow** is explicit and observable:
  - `Block` (default): `PublishAsync` awaits capacity (bounded backpressure to the caller) up to `PublishWaitTimeout`, then throws `OfflineQueueFullException`.
  - `DropOldest`/`DropNewest`: evict + increment `messages.dropped` with a reason.
  - `Reject`: `PublishAsync` throws immediately.
- **Flush ordering:** strict FIFO drain after resubscribe; respects the broker's negotiated **Receive Maximum** inflight window (hard cap here; finer flow-control tuning is Phase 9).
- **No unbounded buffering anywhere** — the explicit `Capacity` is the whole point.

```csharp
public async Task<PublishResult> PublishAsync(MqttApplicationMessage msg, CancellationToken ct)
{
    if (State == ConnectionState.Connected)
        return await _raw.PublishAsync(msg, ct);          // straight through when live

    if (msg.QoS == MqttQoS.AtMostOnce && !_options.OfflineQueue.IncludeQos0)
    {
        _metrics.MessageDropped("offline-qos0");
        return PublishResult.DroppedOffline;
    }
    await _outbound.EnqueueAsync(QueuedPublish.From(msg), ct);  // bounded; may block or throw per policy
    return PublishResult.Queued;
}
```

---

## 4.7 Swap point #5 — `ISessionStore` (resubscribe + inflight restore)

Holds the durable subscription set and QoS 1/2 inflight packet-id state so a reconnect can restore them. Default `InMemorySessionStore`; swap to `SqliteSessionStore` for restart-durable sessions.

```csharp
public interface ISessionStore
{
    ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> filters, CancellationToken ct);
    ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken ct);
    // inflight (QoS1/2) state is shared with Phase 3's state machine via this store
    ValueTask SaveInflightAsync(InflightPacket packet, CancellationToken ct);
    IAsyncEnumerable<InflightPacket> LoadInflightAsync(CancellationToken ct);
    ValueTask ClearInflightAsync(ushort packetId, CancellationToken ct);
}
```

---

## 4.8 The supervisor (orchestrator skeleton)

One owned background loop; sole writer of `ConnectionState`; every external operation is delegated to a swap point. No reflection, fully cancellable, `IAsyncDisposable`.

```csharp
public sealed class ResilientMqttClient : IAsyncDisposable
{
    private readonly RawMqttClient _raw;
    private readonly IReconnectStrategy _reconnect;
    private readonly IConnectionLifecycle _lifecycle;
    private readonly OfflineOutboundQueue _outbound;
    private readonly ISessionStore _session;
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _lifetime = new();
    private Task? _supervisor;

    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    public Task StartAsync(CancellationToken ct)
    {
        _supervisor = Task.Run(() => SuperviseAsync(_lifetime.Token), CancellationToken.None);
        return Task.CompletedTask;          // non-blocking; connection happens in background
    }

    private async Task SuperviseAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                Transition(State == ConnectionState.Disconnected
                    ? ConnectionState.Connecting : ConnectionState.Reconnecting);

                await _reconnect.RunAsync(ConnectOnce, new SupervisorContext(this, _time), ct); // until connected or terminal

                var connAck = _lastConnAck!;
                var upCtx = new ConnectionUpContext(connAck, new SubscriptionRegistrar(_raw, _session));
                await _lifecycle.OnConnectionUpAsync(upCtx, ct);     // resubscribe FIRST
                await _outbound.FlushAsync(_raw, ct);                // THEN flush queue, FIFO

                Transition(ConnectionState.Connected);
                await _raw.WaitForDisconnectAsync(ct);               // serve until dropped
                Transition(ConnectionState.Reconnecting,
                    reason: _raw.LastDisconnectReason);
                await _lifecycle.OnConnectionDownAsync(_raw.LastDisconnectReason, ct);
            }
            catch (TerminalMqttConnectException ex)
            {
                Transition(ConnectionState.Faulted, reason: ex.Reason);
                return;                                              // sticky terminal state
            }
            catch (OperationCanceledException) { break; }           // StopAsync / dispose
        }
    }

    private async Task ConnectOnce(CancellationToken ct)
    {
        _lastConnAck = await _raw.ConnectAsync(_options.ToConnectOptions(), ct); // maps reason→exception (§4.4)
    }

    public async Task StopAsync(CancellationToken ct)
    {
        _lifetime.Cancel();
        if (_supervisor is not null) await _supervisor.WaitAsync(ct);
        await _raw.DisconnectAsync(ct);
        Transition(ConnectionState.Stopped);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        _lifetime.Dispose();
        await _outbound.DisposeAsync();
    }
}
```

---

## 4.9 Sequence walkthroughs

**A. Cold start, broker up**
`Start → Connecting → connectOnce ✓ → OnConnectionUp (no subs yet) → flush (empty) → Connected.`

**B. Mid-session drop**
`Connected → (drop) → Reconnecting → OnConnectionDown → strategy backoff (WaitingRetry…) → connectOnce ✓ → SessionPresent? no → resubscribe from ISessionStore → flush queued publishes FIFO → Connected.`

**C. Publish while offline**
`Connected → (drop) → Reconnecting → user PublishAsync(QoS1) → enqueue (bounded; Block if full) → … reconnect → resubscribe → flush → broker PUBACK → IMessageStore.Acknowledge.`

**D. Terminal auth failure**
`Connecting → connectOnce → CONNACK 0x87 NotAuthorized → IReconnectDecision: Stop → TerminalMqttConnectException → Faulted (sticky).` User fixes credentials, calls `StartAsync` again. **No infinite loop.**

---

## 4.10 Steps → Tasks → Definition of Done

> Generic DoD layers (task/step/phase) are defined in the plan. Below are the *specific* acceptance criteria. All time-dependent tests use `FakeTimeProvider`; all network tests use the **loopback transport** from Phase 2 (no real broker) plus one Mosquitto integration smoke per step.

### Step 4.1 — Connection state machine & supervisor shell
- **T4.1.1** `ConnectionState` enum + `ConnectionStateChanged` + transition method (sole writer). **DoD:** illegal transitions throw in Debug; every transition raises event+metric+log; unit test asserts the full A/B/D transition sequences.
- **T4.1.2** Supervisor loop skeleton with `StartAsync`/`StopAsync`/`DisposeAsync`. **DoD:** start is non-blocking; stop cancels within a bounded timeout; dispose is idempotent; no task leaks (verified with a tracking test).
- **T4.1.3** `WatchState` stream + `StateChanged` event + `State` value. **DoD:** a late subscriber sees current state; stream completes on Stop.
- **Step DoD:** supervisor drives a fake `connectOnce` through Connecting→Connected→Reconnecting→Connected→Stopped deterministically under `FakeTimeProvider`.

### Step 4.2 — `IReconnectStrategy` + default backoff + decision
- **T4.2.1** `IReconnectStrategy`, `ConnectOnceAsync`, `IReconnectContext`, exception types. **DoD:** contracts documented + in `PublicAPI.Shipped`.
- **T4.2.2** `BackoffReconnectStrategy` (exp + full jitter, capped, `TimeProvider`). **DoD:** attempt N delay within `[0, min(cap, base·2^N)]`; infinite by default; cancellation aborts mid-delay; **no wall-clock sleep** (fake-clock test advances virtual time).
- **T4.2.3** `IReconnectDecision` + default reason-code table (§4.4). **DoD:** parametrized test maps each reason code to Retry/Stop; override swaps behavior.
- **T4.2.4** Wire strategy into supervisor; terminal → `Faulted` (sticky). **DoD:** scenario D passes; `Faulted` does not auto-retry; explicit `StartAsync` recovers.
- **Step DoD:** simulated drops on loopback transport recover; NotAuthorized stops without looping; reconnect attempt counter surfaced in state events.

### Step 4.3 — `IConnectionLifecycle` + resubscribe + session restore
- **T4.3.1** `IConnectionLifecycle`, `IConnectionUpContext`, `ISubscriptionRegistrar`. **DoD:** documented; default impl provided.
- **T4.3.2** `ISessionStore` + `InMemorySessionStore`; persist subscription set on Subscribe/Unsubscribe. **DoD:** store reflects current filter set; idempotent re-subscribe.
- **T4.3.3** Default lifecycle: `SessionPresent==false` → replay subs; `==true` → skip. **DoD:** forced drop with `SessionPresent=false` re-subscribes exactly once; with `true`, zero resubscribe calls.
- **T4.3.4** Ordering guarantee: resubscribe completes before flush. **DoD:** test asserts a queued publish to a response topic is sent only after that topic's SUBSCRIBE is acked.
- **Step DoD:** subscriptions survive a forced reconnect; no duplicate SUBSCRIBE when the broker retained the session.

### Step 4.4 — Offline outbound queue
- **T4.4.1** `IMessageStore` + `InMemoryMessageStore` (bounded ring). **DoD:** capacity enforced; FIFO drain; ack removes.
- **T4.4.2** `OfflineOutboundQueue` over bounded `Channel<T>`; `OfflineQueueOptions` with all overflow policies. **DoD:** each policy (Block/DropOldest/DropNewest/Reject) has a dedicated passing test; `messages.dropped` metric increments with reason.
- **T4.4.3** `PublishAsync` routing: live→through, offline→enqueue, QoS0 offline→policy. **DoD:** scenario C passes; QoS0 default-drops with a metric, never silently.
- **T4.4.4** Flush on connect, FIFO, respects negotiated Receive-Maximum cap. **DoD:** inflight never exceeds the negotiated window during flush; order preserved.
- **T4.4.5** Durability seam: same tests pass against a temp-file durable store stub. **DoD:** queued QoS1 publishes survive a simulated restart when a durable store is supplied.
- **Step DoD:** publish-while-offline → reconnect → in-order delivery with acks; bounded under load (no unbounded growth in a soak test).

### Step 4.5 — Polly add-on + swap parity
- **T4.5.1** `Pulse.Mqtt.Resilience.Polly` with `PollyReconnectStrategy`. **DoD:** package builds, AOT-clean, depends only on Core + Polly.
- **T4.5.2** Swap registration recipe + sample. **DoD:** swapping to Polly passes the **unchanged** Step 4.2 acceptance tests (drops recover; terminal exceptions still fault).
- **Step DoD:** default and Polly strategies are interchangeable with identical observable behavior; `NoReconnectStrategy` also passes its (single-attempt) contract test.

### Phase 4 Exit DoD
- Kill the broker mid-run (loopback + Mosquitto): client reconnects, resubscribes, flushes queued publishes in order, emits correct state events, and honors the inflight cap.
- Terminal auth failure faults cleanly (no loop); explicit restart recovers.
- All five swap points (strategy, decision, lifecycle, message store, session store) have a default + at least one alternative, each green against the same contract tests.
- `dotnet build -c Release` clean; full suite green; AOT-publish smoke for Core+Client+Polly shows **zero** trim/AOT warnings; a soak test shows bounded memory.

---

## 4.11 Test matrix (FakeTimeProvider + loopback transport)

| # | Scenario | Setup | Assert |
|---|---|---|---|
| 1 | Cold start success | loopback up | states `Connecting→Connected`; no resubscribe |
| 2 | Backoff timing | connectOnce fails k times | delay_n ∈ [0, min(cap, base·2^n)]; advances only via fake clock |
| 3 | Mid-session drop recovery | drop after Connected | `Connected→Reconnecting→Connected`; OnDown then OnUp fire once each |
| 4 | Resubscribe on `SessionPresent=false` | drop, broker forgets session | exactly one SUBSCRIBE per filter; before any flush |
| 5 | No resubscribe on `SessionPresent=true` | drop, broker retains | zero SUBSCRIBE calls |
| 6 | Publish offline, QoS1 | drop, publish, reconnect | queued→flushed FIFO→PUBACK→store ack; nothing lost |
| 7 | Publish offline, QoS0 default | drop, publish QoS0 | dropped; `messages.dropped{reason=offline-qos0}` +1 |
| 8 | Queue overflow = Block | fill to capacity | `PublishAsync` awaits; throws `OfflineQueueFullException` after timeout |
| 9 | Queue overflow = DropOldest | overfill | oldest evicted; metric +1; newest retained |
| 10 | Terminal auth (0x87) | CONNACK NotAuthorized | `Faulted`; no further attempts; restart recovers |
| 11 | Custom decision (auth=transient) | swap `IReconnectDecision` | retries instead of faulting |
| 12 | Polly strategy parity | swap to Polly | scenarios 2,3,10 pass unchanged |
| 13 | Graceful stop mid-backoff | StopAsync during WaitingRetry | cancels delay; `Stopped`; no leaked task |
| 14 | Durable restart | durable store, queue QoS1, restart | queued messages reloaded and flushed |
| 15 | Soak | 10k drop/reconnect cycles | bounded memory; no handle/task growth |

---

## 4.12 Open design questions (resolve before Phase 4 lock)
1. **Outbound vs inbound backpressure split:** Phase 4 covers outbound (offline queue) + connection backpressure; inbound delivery backpressure is Phase 5 (`IDeliveryPipeline`). Confirm the boundary so Receive-Maximum handling isn't implemented twice.
2. **Reactive surface:** ship an `IObservable<ConnectionStateChanged>` adapter now or as a separate `Pulse.Mqtt.Rx` package? (HiveMQ lead suggests demand; default to a separate package to keep Core lean.)
3. **Credentials refresh coupling:** when `IReconnectDecision` treats auth as transient, it must pair with an `IMqttCredentialsProvider` that actually rotates the token — decide whether to enforce that pairing or just document it.
4. **Durable store ordering vs performance:** strict FIFO durability (SQLite append + fsync) vs batched flush — pick the default durability/throughput trade-off for `SqliteMessageStore` (Phase-4-compatible add-on).
5. **Verify the research leads** (autopaho callbacks, rumqtt capacity model) against primaries before freezing `IConnectionLifecycle`/`OfflineQueueOptions` (see research report §4).
