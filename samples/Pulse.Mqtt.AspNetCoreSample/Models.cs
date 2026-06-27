using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pulse.Mqtt;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.AspNetCoreSample;

internal static class SampleMqttClients
{
    public const string Telemetry = "telemetry";
}

public sealed record TelemetryPublishRequest(
    string Unit,
    double Value,
    DateTimeOffset? Timestamp = null);

public sealed record TelemetryReading(
    string Unit,
    double Value,
    DateTimeOffset Timestamp);

public sealed record RoutedTelemetry(
    string DeviceId,
    string Topic,
    TelemetryReading Reading,
    DateTimeOffset ReceivedAt);

public sealed record TelemetryPublishResponse(
    string DeviceId,
    string Topic,
    string Disposition,
    string? ReasonCode);

public sealed record MqttDiagnosticsResponse(
    string ClientId,
    string State,
    int Attempt,
    bool IsRunning,
    DateTimeOffset StateChangedAt,
    string? LastReason,
    string? LastReasonString,
    string? LastServerReference,
    string? LastError,
    int? OfflineQueueDepth,
    long? OfflineQueueDroppedCount,
    int SubscriptionCount,
    int PendingSubscribeCount,
    int PendingUnsubscribeCount,
    MqttCapabilitiesResponse? BrokerCapabilities)
{
    public static MqttDiagnosticsResponse From(MqttClientDiagnosticsSnapshot snapshot) => new(
        snapshot.ClientId,
        snapshot.State.ToString(),
        snapshot.Attempt,
        snapshot.IsRunning,
        snapshot.StateChangedAt,
        snapshot.LastReason?.ToString(),
        snapshot.LastReasonString,
        snapshot.LastServerReference,
        snapshot.LastError?.Message,
        snapshot.OfflineQueueDepth,
        snapshot.OfflineQueueDroppedCount,
        snapshot.SubscriptionCount,
        snapshot.PendingSubscribeCount,
        snapshot.PendingUnsubscribeCount,
        MqttCapabilitiesResponse.From(snapshot.BrokerCapabilities));
}

public sealed record MqttCapabilitiesResponse(
    string? ProtocolVersion,
    bool IsConnected,
    string TopicAliases,
    ushort EffectiveTopicAliasMaximum,
    string SubscriptionIdentifiers,
    string SharedSubscriptions,
    string RetainedMessages,
    string WildcardSubscriptions,
    string EffectiveMaximumQoS,
    ushort? EffectiveReceiveMaximum,
    bool CanUseMqtt5RequestResponse,
    bool CanUseTopicAliases)
{
    public static MqttCapabilitiesResponse? From(MqttBrokerCapabilitiesSnapshot? capabilities)
    {
        if (capabilities is null)
        {
            return new MqttCapabilitiesResponse(
                ProtocolVersion: null,
                IsConnected: false,
                TopicAliases: MqttBrokerFeatureSupport.Unknown.ToString(),
                EffectiveTopicAliasMaximum: 0,
                SubscriptionIdentifiers: MqttBrokerFeatureSupport.Unknown.ToString(),
                SharedSubscriptions: MqttBrokerFeatureSupport.Unknown.ToString(),
                RetainedMessages: MqttBrokerFeatureSupport.Unknown.ToString(),
                WildcardSubscriptions: MqttBrokerFeatureSupport.Unknown.ToString(),
                EffectiveMaximumQoS: MqttQualityOfService.AtMostOnce.ToString(),
                EffectiveReceiveMaximum: null,
                CanUseMqtt5RequestResponse: false,
                CanUseTopicAliases: false);
        }

        return new MqttCapabilitiesResponse(
            capabilities.ProtocolVersion.ToString(),
            IsConnected: true,
            capabilities.TopicAliases.ToString(),
            capabilities.EffectiveTopicAliasMaximum,
            capabilities.SubscriptionIdentifiers.ToString(),
            capabilities.SharedSubscriptions.ToString(),
            capabilities.RetainedMessages.ToString(),
            capabilities.WildcardSubscriptions.ToString(),
            capabilities.EffectiveMaximumQoS.ToString(),
            capabilities.EffectiveReceiveMaximum,
            CanUseMqtt5RequestResponse: capabilities.ProtocolVersion == MqttProtocolVersion.V500,
            CanUseTopicAliases: capabilities.TopicAliases == MqttBrokerFeatureSupport.Supported);
    }
}

public sealed class TelemetryStore
{
    private readonly ConcurrentDictionary<string, RoutedTelemetry> _latestByDevice = new(StringComparer.Ordinal);

    public void Record(string deviceId, string topic, TelemetryReading reading)
    {
        _latestByDevice[deviceId] = new RoutedTelemetry(deviceId, topic, reading, DateTimeOffset.UtcNow);
    }

    public bool TryGet(string deviceId, out RoutedTelemetry reading) =>
        _latestByDevice.TryGetValue(deviceId, out reading!);
}

public sealed class TelemetryRouteService(
    IPulseMqttClientFactory clients,
    TelemetryStore store,
    ILogger<TelemetryRouteService> logger) : IHostedService
{
    private MqttSubscribedRoute? _route;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var client = clients.GetClient(SampleMqttClients.Telemetry);
        _route = await client.OnAsync<TelemetryReading>(
            "sensors/{deviceId}/telemetry",
            MqttQualityOfService.AtLeastOnce,
            (reading, message, _) =>
            {
                var deviceId = message.Values["deviceId"];
                store.Record(deviceId, message.Message.Topic, reading);
                logger.LogInformation("Routed telemetry for {DeviceId} from {Topic}", deviceId, message.Message.Topic);
                return ValueTask.CompletedTask;
            },
            cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_route is not null)
        {
            await _route.DisposeAsync();
        }
    }
}

[JsonSerializable(typeof(TelemetryReading))]
public sealed partial class AspNetSampleJsonContext : JsonSerializerContext;
