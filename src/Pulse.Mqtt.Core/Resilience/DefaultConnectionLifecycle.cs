using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>
/// The default lifecycle: when the broker did not retain the session, replays the subscription set
/// stored in the <see cref="ISessionStore"/>; when it did, does nothing. Idempotent and safe to
/// run on every reconnect.
/// </summary>
public sealed class DefaultConnectionLifecycle : IConnectionLifecycle
{
    private readonly ISessionStore _sessionStore;

    /// <summary>Creates the lifecycle over the given session store.</summary>
    public DefaultConnectionLifecycle(ISessionStore sessionStore)
    {
        ArgumentNullException.ThrowIfNull(sessionStore);
        _sessionStore = sessionStore;
    }

    /// <inheritdoc />
    public async ValueTask OnConnectionUpAsync(IConnectionUpContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.ConnAck.SessionPresent)
        {
            return;
        }

        var subscriptions = await _sessionStore.LoadSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
        if (subscriptions.Count > 0)
        {
            await context.Subscriptions.SubscribeAsync(subscriptions, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask OnConnectionDownAsync(MqttReasonCode? reason, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
}
