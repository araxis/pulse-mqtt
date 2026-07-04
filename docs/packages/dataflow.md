# Dataflow package

Package: `Pulse.Mqtt.Dataflow`

Use this package when MQTT input should feed bounded `System.Threading.Tasks.Dataflow` source
blocks for worker pipelines.

## Install

```shell
dotnet add package Pulse.Mqtt.Dataflow
```

The package targets the same runtime frameworks as the client package and plugs into an existing
`ResilientMqttClient`. It does not change broker subscriptions or MQTT wire behavior.

## Choose a source block

| Method | Produces | Use when |
| --- | --- | --- |
| `ToMessageSourceBlock` | `MqttPublishPacket` | A pipeline owns the raw inbound message stream. |
| `ToRouteSourceBlock` | `MqttRoutedMessage` | A pipeline should receive only messages matching one route template. |
| `ToAcknowledgedRouteSourceBlock` | `MqttAcknowledgedRoutedMessage` | Processing decides when to `AcknowledgeAsync` or `RejectAsync`. |
| `ToStateSourceBlock` | `ConnectionStateChanged` | State transitions should feed logging, metrics, or supervision blocks. |

`MqttDataflowSourceOptions.BoundedCapacity` defaults to `256` and must be positive.

## Route source block

Route source blocks are local adapters. They do not send MQTT `SUBSCRIBE`; call `SubscribeAsync`
first so the broker delivers matching traffic.

```csharp
using Pulse.Mqtt.Dataflow;
using System.Threading.Tasks.Dataflow;

var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");
await client.SubscribeAsync(template, MqttQualityOfService.AtLeastOnce, token);

await using var source = client.ToRouteSourceBlock(
    template,
    sourceOptions: new MqttDataflowSourceOptions { BoundedCapacity = 128 },
    cancellationToken: token);

var worker = new ActionBlock<MqttRoutedMessage>(
    routed => ProcessAsync(routed, token),
    new ExecutionDataflowBlockOptions
    {
        BoundedCapacity = 64,
        MaxDegreeOfParallelism = 4,
    });

using var link = source.LinkTo(
    worker,
    new DataflowLinkOptions { PropagateCompletion = true });
```

## Acknowledged route source

Use acknowledged route blocks when application work must finish before the broker receives the
message acknowledgement:

```csharp
await using var source = client.ToAcknowledgedRouteSourceBlock(template, cancellationToken: token);

var worker = new ActionBlock<MqttAcknowledgedRoutedMessage>(async routed =>
{
    try
    {
        await PersistAsync(routed.Message, token);
        await routed.AcknowledgeAsync(token);
    }
    catch when (routed.CanReject)
    {
        await routed.RejectAsync(MqttReasonCode.UnspecifiedError, cancellationToken: token);
    }
});
```

`RejectAsync` is available only when `CanReject` is true. MQTT 3.1.1 cannot carry per-message
negative acknowledgement reason codes.

## Completion and disposal

- Source blocks complete when their underlying client stream completes.
- Pump failures fault the source block.
- `DisposeAsync` stops the pump and releases any owned route registration.
- `Complete()` stops the source and completes the wrapped buffer.
- Raw message sources consume `client.Messages`; do not read `client.Messages` elsewhere at the
  same time.

## Related docs

- [Routing](/guide/routing#dataflow-source-blocks)
- [Subscribing](/guide/subscribing#consuming-the-raw-message-stream)
- [Lifecycle and state](/guide/lifecycle)
- [Worker sample](https://github.com/araxis/pulse-mqtt/tree/main/samples/Pulse.Mqtt.WorkerSample)
