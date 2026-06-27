# Dataflow package

Package: `Pulse.Mqtt.Dataflow`

Use this package when MQTT messages should feed bounded source blocks for worker pipelines.

## Install

```shell
dotnet add package Pulse.Mqtt.Dataflow
```

## Route source block

```csharp
using Pulse.Mqtt.Dataflow;
using System.Threading.Tasks.Dataflow;

var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

await using var source = client.ToRouteSourceBlock(
    template,
    sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 128 },
    cancellationToken: token);

using var link = source.LinkTo(
    new ActionBlock<MqttRoutedMessage>(
        routed => ProcessAsync(routed, token),
        new ExecutionDataflowBlockOptions { BoundedCapacity = 64 }),
    new DataflowLinkOptions { PropagateCompletion = true });
```

## Available sources

| Method | Produces | Notes |
| --- | --- | --- |
| `ToMessageSourceBlock` | `MqttPublishPacket` | Consumes `client.Messages`; do not read the raw stream elsewhere at the same time. |
| `ToRouteSourceBlock` | `MqttRoutedMessage` | Local route adapter; call `SubscribeAsync` separately. |
| `ToAcknowledgedRouteSourceBlock` | `MqttAcknowledgedRoutedMessage` | Consumer must call `AcknowledgeAsync` or `RejectAsync`. |
| `ToStateSourceBlock` | `ConnectionStateChanged` | Streams connection transitions. |

`MqttDataflowSourceOptions.BoundedCapacity` defaults to `256` and must be positive. Source blocks
complete when the underlying stream completes and fault when the pump fails.

See [Routing](/guide/routing#dataflow-source-blocks), [Subscribing](/guide/subscribing#consuming-the-raw-message-stream),
and [Lifecycle and state](/guide/lifecycle) for usage details.

The runnable worker sample combines `SubscribeAsync`, `ToRouteSourceBlock`, bounded processing
stages, capability checks, and graceful shutdown:
[`samples/Pulse.Mqtt.WorkerSample`](https://github.com/araxis/pulse-mqtt/tree/main/samples/Pulse.Mqtt.WorkerSample).
