# Pulse.Mqtt.Dataflow

Source blocks for routing Pulse MQTT messages into `System.Threading.Tasks.Dataflow` pipelines.

## Install

```shell
dotnet add package Pulse.Mqtt.Dataflow
```

## Source block options

```csharp
await using var source = client.ToRouteSourceBlock(
    MqttRouteTemplate.Parse("telemetry/{deviceId}"),
    sourceOptions: new MqttDataflowSourceOptions
    {
        BoundedCapacity = 128,
    },
    cancellationToken: cancellationToken);
```

## Route into a worker block

```csharp
var worker = new ActionBlock<MqttRoutedMessage>(
    message => ProcessAsync(message, cancellationToken),
    new ExecutionDataflowBlockOptions
    {
        BoundedCapacity = 64,
        MaxDegreeOfParallelism = 4,
    });

using var link = source.LinkTo(
    worker,
    new DataflowLinkOptions { PropagateCompletion = true });
```

## Manual acknowledgement source

Use acknowledged route source blocks when application work decides when the broker is acknowledged.

```csharp
await using var source = client.ToAcknowledgedRouteSourceBlock(
    MqttRouteTemplate.Parse("orders/{id}"),
    cancellationToken: cancellationToken);

var worker = new ActionBlock<MqttAcknowledgedRoutedMessage>(async routed =>
{
    await PersistAsync(routed.Message, cancellationToken);
    await routed.AcknowledgeAsync(cancellationToken);
});
```

Source blocks are local adapters. They do not send broker subscriptions by themselves unless the helper explicitly says it owns the subscription.

Full docs: https://araxis.github.io/pulse-mqtt/packages/dataflow
