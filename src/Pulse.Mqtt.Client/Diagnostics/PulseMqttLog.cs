using Microsoft.Extensions.Logging;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Client;

/// <summary>Source-generated log messages for the resilient client. Allocation-free when disabled.</summary>
internal static partial class PulseMqttLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information,
        Message = "MQTT client {ClientId} state {Previous} -> {Current} (attempt {Attempt})")]
    public static partial void StateChanged(
        ILogger logger, string clientId, ConnectionState previous, ConnectionState current, int attempt);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning,
        Message = "MQTT client {ClientId} connect attempt {Attempt} failed")]
    public static partial void ConnectAttemptFailed(ILogger logger, string clientId, int attempt, Exception error);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information,
        Message = "MQTT client {ClientId} lost its connection")]
    public static partial void ConnectionLost(ILogger logger, string clientId);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error,
        Message = "MQTT route {Template} handler failed")]
    public static partial void RouteHandlerFaulted(ILogger logger, string template, Exception error);

    [LoggerMessage(EventId = 5, Level = LogLevel.Warning,
        Message = "MQTT client {ClientId} was disconnected by the broker: {Reason}")]
    public static partial void ServerDisconnected(ILogger logger, string clientId, MqttReasonCode reason);

    [LoggerMessage(EventId = 6, Level = LogLevel.Warning,
        Message = "MQTT client {ClientId} dropped a queued publish to {Topic}: {PacketSize} bytes exceeds the broker's {Limit}-byte maximum")]
    public static partial void QueuedPublishTooLarge(ILogger logger, string clientId, string topic, int packetSize, uint limit);

    [LoggerMessage(EventId = 7, Level = LogLevel.Warning,
        Message = "MQTT client {ClientId} failed to publish its birth message to {Topic}")]
    public static partial void BirthPublishFailed(ILogger logger, string clientId, string topic, Exception error);

    [LoggerMessage(EventId = 8, Level = LogLevel.Warning,
        Message = "MQTT client {ClientId} discarded {Count} in-flight publish(es): the broker did not preserve the session")]
    public static partial void InFlightDiscarded(ILogger logger, string clientId, int count);

    [LoggerMessage(EventId = 9, Level = LogLevel.Information,
        Message = "MQTT client {ClientId} dropped a queued publish to {Topic}: its {ExpirySeconds}s message expiry elapsed while offline")]
    public static partial void QueuedPublishExpired(ILogger logger, string clientId, string topic, uint expirySeconds);

    [LoggerMessage(EventId = 10, Level = LogLevel.Information,
        Message = "MQTT client {ClientId} is following a server redirect to {Host}:{Port} (hop {Hop})")]
    public static partial void FollowingServerRedirect(ILogger logger, string clientId, string host, int? port, int hop);

    [LoggerMessage(EventId = 11, Level = LogLevel.Warning,
        Message = "MQTT client {ClientId} reached its redirect bound of {MaxServerRedirects} consecutive hops; treating the next redirect as terminal")]
    public static partial void ServerRedirectBoundReached(ILogger logger, string clientId, int maxServerRedirects);

    [LoggerMessage(EventId = 12, Level = LogLevel.Information,
        Message = "MQTT client {ClientId}: the opportunistic offline-queue flush after a queued publish failed; the message stays queued and the next connection-up flush will drain it")]
    public static partial void QueuedPublishNudgeFailed(ILogger logger, string clientId, Exception error);
}
