# Extending the client

Every major behavior is a small interface with a solid default. This page is the catalog: what
each contract owns, when to replace it, and a working sketch for each.

With DI, each swap is one builder call; with direct construction, set the matching
`ResilientMqttClientOptions` property.

## Custom reconnect strategy

**Contract:** `IReconnectStrategy` — owns the retry loop around a single connect attempt.
**Default:** exponential backoff with full jitter ([`BackoffOptions`](/reference/options#backoffoptions)).

The Polly add-on is the canonical swap:

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

Writing your own:

```csharp
public sealed class FixedIntervalStrategy(TimeSpan interval) : IReconnectStrategy
{
    public async Task RunAsync(ConnectOnceAsync connectOnce, IReconnectContext context, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            context.OnAttemptStarting(attempt);
            try { await connectOnce(ct); return; }
            catch (OperationCanceledException) { throw; }
            catch (TerminalMqttConnectException) { throw; }   // respect terminal classification
            catch (Exception error)
            {
                context.OnAttemptFailed(attempt, error);
                await Task.Delay(interval, context.Time, ct); // context.Time: testable clock
            }
        }
    }
}
```

Rules: rethrow `OperationCanceledException` and `TerminalMqttConnectException`; use
`context.Time` for delays so tests stay fast.

## Custom reconnect decision

**Contract:** `IReconnectDecision` — classifies a failed attempt as retryable or final.
**Default:** authentication and identity rejections are final; network errors retry.

```csharp
public sealed class TokenAwareDecision(ITokenSource tokens) : IReconnectDecision
{
    public bool ShouldRetry(int attempt, Exception error) =>
        error is MqttConnectRejectedException { ReasonCode: MqttReasonCode.NotAuthorized }
            ? tokens.CanRefresh        // expired token: refresh and retry
            : attempt < 100;
}
```

Returning `false` faults the client **sticky** — recovery is an explicit `StartAsync`.

## Custom lifecycle

**Contract:** `IConnectionLifecycle` — runs on connection up and down.
**Default:** re-subscribes the stored subscription set when the broker session was lost.

```csharp
public sealed class WarmCacheLifecycle(IConnectionLifecycle inner, ICache cache) : IConnectionLifecycle
{
    public async ValueTask OnConnectionUpAsync(ConnectionUpContext context, CancellationToken ct)
    {
        await inner.OnConnectionUpAsync(context, ct);   // keep the default re-subscription
        await cache.WarmAsync(ct);
    }

    public ValueTask OnConnectionDownAsync(Exception? error, CancellationToken ct) =>
        inner.OnConnectionDownAsync(error, ct);
}
```

Decorate the default rather than replacing it unless you genuinely own re-subscription:
the up-context exposes the CONNACK, the attempt number, and a subscription registrar.

## Custom session store

**Contract:** `ISessionStore` — the durable subscription set.
**Default:** in-memory (process lifetime).

```csharp
public sealed class SqliteSessionStore(string connectionString) : ISessionStore
{
    public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> filters, CancellationToken ct) { /* replace all */ }
    public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken ct) { /* read */ }
    public ValueTask ClearAsync(CancellationToken ct) { /* delete */ }

    // Optional but recommended: the interface ships default implementations of
    // UpsertSubscriptionsAsync / RemoveSubscriptionsAsync that rewrite the full set.
    // Override both for in-place updates when subscription counts are large.
}
```

## Custom message store

**Contract:** `IMessageStore` — the offline publish queue.
**Default:** bounded in-memory with explicit overflow.

A durable implementation (SQLite, LiteDB, files) gives queued publishes restart survival.
Honor the contract's queue semantics: append, peek oldest, remove head — the supervisor
flushes oldest-first after re-subscription.

## Custom serializer

**Contract:** `IMqttSerializer` — typed payloads. **Default:** none (typed APIs throw until
one is configured). Full guide: [Typed messaging](./typed-messaging#bring-your-own-format).

## Custom transport

**Contract:** `IMqttTransportFactory` → `IMqttTransport` (a `PipeReader`/`PipeWriter` pair).
**Default:** TCP with optional TLS; WebSocket ships as an add-on.

```csharp
public sealed class NamedPipeTransportFactory(string pipeName) : IMqttTransportFactory
{
    public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken ct)
    {
        var stream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        await stream.ConnectAsync(ct);
        return new StreamTransport(stream);   // PipeReader.Create(stream) / PipeWriter.Create(stream)
    }
}
```

The factory is called for **every** connection attempt — return a fresh transport each time.
The [test broker](./testing#the-in-process-broker) is itself just a transport factory.

## Composition reminders

- Swaps are **per client**; different named clients can mix freely.
- Each contract has one reason to change — implement only what you need, defaults handle the
  rest.
- All of them resolve through DI with the provider available, so your implementations can have
  dependencies of their own.
