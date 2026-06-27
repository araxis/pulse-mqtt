using System.Text.Json.Serialization;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Dataflow;
using Pulse.Mqtt.DependencyInjection;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Routing;
using Pulse.Mqtt.Serialization.Json;

namespace Pulse.Mqtt.WorkerSample;

internal static class WorkerMqttClients
{
    public const string Telemetry = "telemetry";
}

public sealed record TelemetryReading(
    string Unit,
    double Value,
    DateTimeOffset Timestamp);

public sealed record ProcessedTelemetry(
    string DeviceId,
    string Topic,
    TelemetryReading Reading,
    DateTimeOffset ProcessedAt);

public sealed class TelemetryPipelineWorker(
    IPulseMqttClientFactory clients,
    IHostApplicationLifetime lifetime,
    ILogger<TelemetryPipelineWorker> logger) : BackgroundService
{
    private const int MessageCount = 5;
    private readonly JsonMqttSerializer _serializer = new(WorkerSampleJsonContext.Default);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var client = clients.GetClient(WorkerMqttClients.Telemetry);
        var template = MqttRouteTemplate.Parse("sensors/{deviceId}/telemetry");

        await client.SubscribeAsync([template.ToTopicFilter(MqttQualityOfService.AtLeastOnce)], stoppingToken)
            .ConfigureAwait(false);
        await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(10), stoppingToken)
            .ConfigureAwait(false);

        LogCapabilities(client);

        var processedEnough = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processedCount = 0;

        await using var source = client.ToRouteSourceBlock(
            template,
            sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 32 },
            cancellationToken: stoppingToken);

        var parse = new TransformBlock<MqttRoutedMessage, ProcessedTelemetry>(
            message =>
            {
                var reading = _serializer.Deserialize<TelemetryReading>(message.Message.Payload);
                return new ProcessedTelemetry(
                    message.Values["deviceId"],
                    message.Message.Topic,
                    reading,
                    DateTimeOffset.UtcNow);
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 16,
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                EnsureOrdered = false,
                CancellationToken = stoppingToken,
            });

        var persist = new ActionBlock<ProcessedTelemetry>(
            item =>
            {
                logger.LogInformation(
                    "Processed {DeviceId} from {Topic}: {Value:F1}{Unit}",
                    item.DeviceId,
                    item.Topic,
                    item.Reading.Value,
                    item.Reading.Unit);

                if (Interlocked.Increment(ref processedCount) >= MessageCount)
                {
                    processedEnough.TrySetResult();
                }
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 8,
                MaxDegreeOfParallelism = 1,
                CancellationToken = stoppingToken,
            });

        using var linkToParse = source.LinkTo(parse, new DataflowLinkOptions { PropagateCompletion = true });
        using var linkToPersist = parse.LinkTo(persist, new DataflowLinkOptions { PropagateCompletion = true });

        try
        {
            await PublishSeedMessagesAsync(client, stoppingToken).ConfigureAwait(false);
            await processedEnough.Task.WaitAsync(stoppingToken).ConfigureAwait(false);

            source.Complete();
            parse.Complete();
            await persist.Completion.WaitAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            lifetime.StopApplication();
        }
    }

    private async Task PublishSeedMessagesAsync(ResilientMqttClient client, CancellationToken cancellationToken)
    {
        for (var index = 1; index <= MessageCount; index++)
        {
            var reading = new TelemetryReading("C", 20 + index, DateTimeOffset.UtcNow);
            await client.PublishAsync(
                    $"sensors/device-{index}/telemetry",
                    reading,
                    MqttQualityOfService.AtLeastOnce,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private void LogCapabilities(ResilientMqttClient client)
    {
        var capabilities = client.GetBrokerCapabilitiesSnapshot();
        var canUseRequestResponse = capabilities?.ProtocolVersion == MqttProtocolVersion.V500;
        var canUseTopicAliases = capabilities?.TopicAliases == MqttBrokerFeatureSupport.Supported;

        logger.LogInformation(
            "Connected with {ProtocolVersion}; request/response={RequestResponse}; topic aliases={TopicAliases}",
            capabilities?.ProtocolVersion.ToString() ?? "unknown",
            canUseRequestResponse,
            canUseTopicAliases);
    }
}

[JsonSerializable(typeof(TelemetryReading))]
public sealed partial class WorkerSampleJsonContext : JsonSerializerContext;
