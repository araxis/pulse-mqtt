# Extending the client

Every major behavior is a small interface with a solid default. This page is the reference:
the exact contract for each swap point, when to replace it, and a complete worked
implementation. None of them use reflection, so your implementations stay Native AOT safe.

With dependency injection each swap is one builder call (`Use*`); with direct construction set
the matching `ResilientMqttClientOptions` property or pass it to `PulseMqttClientBuilder`. Swaps
are **per named client**, so two clients can mix freely.

## At a glance

| Swap point | Contract | Default | DI method | Option |
| --- | --- | --- | --- | --- |
| Transport | `IMqttTransportFactory` | TCP/TLS | `UseTransportFactory` | (factory ctor arg) |
| Reconnect loop | `IReconnectStrategy` | Backoff + jitter | `UseReconnectStrategy` | `ReconnectStrategy` |
| Retry classification | `IReconnectDecision` | Auth-final | `UseReconnectDecision` | `ReconnectDecision` |
| Connection hooks | `IConnectionLifecycle` | Re-subscribe | `UseLifecycle` | `Lifecycle` |
| Session store | `ISessionStore` | In-memory | `UseSessionStore` | `SessionStore` |
| Offline queue | `IMessageStore` | Bounded in-memory | `UseMessageStore` | `MessageStore` |
| Serializer | `IMqttSerializer` | none | `UseSerializer` | `Serializer` |

## Reconnect strategy

**Contract.** The supervisor hands you a "connect once" delegate; you decide how to retry it.

```csharp
public delegate Task ConnectOnceAsync(CancellationToken cancellationToken);

public interface IReconnectStrategy
{
    Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken cancellationToken);
}

public interface IReconnectContext
{
    int Attempt { get; }
    TimeProvider Time { get; }                       // use this for delays — keeps tests fast
    void OnAttemptStarting(int attempt);
    void OnAttemptFailed(int attempt, Exception error);
}
```

`RunAsync` returns when a connection succeeds, throws `TerminalMqttConnectException` (or rethrows
`OperationCanceledException`) when it gives up, and reports each attempt through `context`.

**Default:** exponential backoff with full jitter ([`BackoffOptions`](/reference/options#backoffoptions)),
retrying indefinitely.

**The Polly add-on** is the canonical swap:

```csharp
.UseReconnectStrategy(_ => new PollyReconnectStrategy(
    new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            MaxRetryAttempts = int.MaxValue,
        })
        .Build()))
```

**A fixed-interval strategy from scratch:**

```csharp
public sealed class FixedIntervalStrategy(TimeSpan interval) : IReconnectStrategy
{
    public async Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            context.OnAttemptStarting(attempt);
            try
            {
                await connectOnce(ct);
                return;                                         // connected — done
            }
            catch (OperationCanceledException) { throw; }       // shutdown — propagate
            catch (TerminalMqttConnectException) { throw; }     // decision said stop — propagate
            catch (Exception error)
            {
                context.OnAttemptFailed(attempt, error);
                await Task.Delay(interval, context.Time, ct);   // context.Time, never Task.Delay(interval, ct)
            }
        }
    }
}
```

Rules: rethrow `OperationCanceledException` and `TerminalMqttConnectException` unchanged, and
take every delay from `context.Time` so a `FakeTimeProvider` makes reconnection tests instant.

## Reconnect decision

**Contract.**

```csharp
public interface IReconnectDecision
{
    bool ShouldRetry(int attempt, Exception error);
}
```

**Default:** authentication/identity rejections are terminal; network errors retry. Returning
`false` faults the client **sticky** — recovery is an explicit `ConnectAsync`.

**Token-aware:** treat `NotAuthorized` as transient while a token can refresh out of band:

```csharp
public sealed class TokenAwareDecision(ITokenSource tokens) : IReconnectDecision
{
    public bool ShouldRetry(int attempt, Exception error) => error switch
    {
        MqttConnectRejectedException { ReasonCode: MqttReasonCode.NotAuthorized } => tokens.CanRefresh,
        MqttConnectRejectedException { ReasonCode: MqttReasonCode.BadUserNameOrPassword } => false,
        _ => attempt < 50,
    };
}
```

## Connection lifecycle

**Contract.** Hooks that run as a connection comes up (before any traffic) and goes down.

```csharp
public interface IConnectionLifecycle
{
    ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken cancellationToken);
    ValueTask OnConnectionDownAsync(IConnectionDownContext context, CancellationToken cancellationToken);
}

public interface IConnectionUpContext
{
    MqttConnAckPacket ConnAck { get; }            // SessionPresent, negotiated limits
    int Attempt { get; }
    ISubscriptionRegistrar Subscriptions { get; } // re-establish subscriptions on the live link
}

public interface IConnectionDownContext
{
    MqttReasonCode? Reason { get; }               // the broker's DISCONNECT reason, when it sent one
    string? ReasonString { get; }
    string? ServerReference { get; }              // redirect target, for redirect-aware deployments
    Exception? Error { get; }                     // what ended the session, when known
}

public interface ISubscriptionRegistrar
{
    ValueTask<IReadOnlyList<MqttReasonCode>> SubscribeAsync(
        IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken);
}
```

`OnConnectionUpAsync` runs **after CONNACK and before the offline queue flushes**, so whatever
you do here is in place before live traffic.

**Default** (`DefaultConnectionLifecycle`): when `ConnAck.SessionPresent` is false, it loads the
stored subscription set and replays it; when the broker kept the session, it does nothing.

**Decorate it** to keep that re-subscription and add priming — the cleanest approach, since the
default already owns the subtle ordering:

```csharp
public sealed class WarmCacheLifecycle(ISessionStore sessions, ICache cache) : IConnectionLifecycle
{
    private readonly DefaultConnectionLifecycle _inner = new(sessions);

    public async ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken ct)
    {
        await _inner.OnConnectionUpAsync(context, ct);    // keep the default re-subscription
        await cache.WarmAsync(ct);                         // then your priming
    }

    public ValueTask OnConnectionDownAsync(IConnectionDownContext context, CancellationToken ct) =>
        _inner.OnConnectionDownAsync(context, ct);
}
```

If you own re-subscription entirely (custom ordering, extra filters), call
`context.Subscriptions.SubscribeAsync(...)` yourself instead of delegating.

## Session store

**Contract.** Holds the durable subscription set.

```csharp
public interface ISessionStore
{
    ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken);
    ValueTask ClearAsync(CancellationToken cancellationToken);

    // Default implementations rewrite the whole set; override for in-place updates at scale.
    ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken);
    ValueTask RemoveSubscriptionsAsync(IReadOnlyList<string> topics, CancellationToken cancellationToken);
}
```

**Default:** in-memory (process lifetime). A durable implementation (SQLite, LiteDB, files)
restores subscriptions across restarts. The `Upsert`/`Remove` methods ship with default
implementations built on `Load` + `Save`; override both when subscription counts are high enough
that rewriting the whole set per call hurts (the in-memory store does exactly this).

The store also persists the session's **in-flight QoS state** for
[redelivery on resume](./resilience#in-flight-redelivery-on-session-resume) through
`SaveInFlightAsync`/`LoadInFlightAsync` (an `MqttInFlightState` of unfinished outbound exchanges
and inbound QoS 2 identifiers). Both have default no-op implementations, so a store that does
not override them simply opts out of cross-restart redelivery; override them in a durable store
to carry in-flight work across process restarts.

## Message store

**Contract.** The bounded offline publish queue.

```csharp
public interface IMessageStore
{
    int Count { get; }
    long DroppedCount { get; }                         // never silent — surfaced for dashboards
    ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken cancellationToken);
    ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken cancellationToken);   // oldest, no removal
    ValueTask RemoveHeadAsync(CancellationToken cancellationToken);                 // after a successful flush
    ValueTask ClearAsync(CancellationToken cancellationToken);
}
```

**Default:** bounded in-memory with the four [overflow policies](./resilience#the-offline-queue).
The flush loop is **peek → send → remove-head**, so a crash between send and remove re-sends
rather than loses (at-least-once). Honor that contract in a durable store: `PeekAsync` must
return the oldest without removing it, and `RemoveHeadAsync` removes exactly that one.

```csharp
public sealed class SqliteMessageStore : IMessageStore
{
    public int Count => /* SELECT COUNT(*) */;
    public long DroppedCount => /* a counter you maintain on overflow */;

    public ValueTask EnqueueAsync(MqttPublishPacket packet, CancellationToken ct) { /* INSERT, applying overflow */ }
    public ValueTask<MqttPublishPacket?> PeekAsync(CancellationToken ct) { /* SELECT ... ORDER BY seq LIMIT 1 */ }
    public ValueTask RemoveHeadAsync(CancellationToken ct) { /* DELETE the row Peek returned */ }
    public ValueTask ClearAsync(CancellationToken ct) { /* DELETE FROM queue */ }
}
```

Encode the publish (topic, QoS, retain, payload, properties) however you like — the codec in
`Pulse.Mqtt.Core` can serialize an `MqttPublishPacket` to bytes if you want wire format on disk.

## Serializer

**Contract.**

```csharp
public interface IMqttSerializer
{
    string ContentType { get; }                        // stamped on typed publishes
    MqttPayloadFormatIndicator PayloadFormat { get; }  // stamped on typed publishes
    ReadOnlyMemory<byte> Serialize<T>(T value);
    T Deserialize<T>(ReadOnlyMemory<byte> payload);    // throws MqttException on a bad payload
}
```

**Default:** none — the typed APIs throw `InvalidOperationException` until one is configured.
The JSON add-on is source-generated and AOT-safe. Full walkthrough, including a MessagePack
example, in [Typed messaging](./typed-messaging#bring-your-own-format).

## Transport

**Contract.** A factory that produces a connected byte transport — a `PipeReader`/`PipeWriter`
pair — per connection attempt.

```csharp
public interface IMqttTransportFactory
{
    ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken);
}

public interface IMqttTransport : IAsyncDisposable
{
    PipeReader Input { get; }
    PipeWriter Output { get; }
}
```

**Default:** TCP with optional TLS; WebSocket ships as an add-on. `ConnectAsync` is called for
**every** attempt, so return a fresh transport each time — the resilient client disposes the old
one on disconnect.

```csharp
public sealed class NamedPipeTransportFactory(string pipeName) : IMqttTransportFactory
{
    public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken ct)
    {
        var stream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        await stream.ConnectAsync(ct);
        return new StreamTransport(stream);   // wrap PipeReader.Create / PipeWriter.Create (leaveOpen: true)
    }
}
```

The in-process [test broker](./testing#the-in-process-broker) is itself just an
`IMqttTransportFactory` — the cleanest reference implementation to read.

## Composition reminders

- Swaps are per client; different named clients can mix any combination.
- Each contract has one reason to change — implement only what you need, defaults handle the
  rest.
- Every `Use*` factory receives the `IServiceProvider`, so your implementations can take
  dependencies of their own.
- Nothing here uses reflection, so a fully-swapped client still publishes Native AOT with zero
  warnings.
