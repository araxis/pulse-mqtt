# Pulse MQTT worker/Dataflow sample

This sample is a small worker service that turns MQTT routed messages into a bounded Dataflow
pipeline. It demonstrates:

- resilient client registration through `AddPulseMqttClient`
- explicit broker subscription ownership with `SubscribeAsync`
- `ToRouteSourceBlock` from `Pulse.Mqtt.Dataflow`
- bounded Dataflow source, parse, and processing stages
- protocol feature guard checks after connection
- graceful completion and host shutdown

Run it with the in-process test broker:

```shell
dotnet run --project samples/Pulse.Mqtt.WorkerSample
```

Run it against a real broker:

```shell
dotnet run --project samples/Pulse.Mqtt.WorkerSample -- --Mqtt:Host localhost --Mqtt:Port 1883
```

Use MQTT 3.1.1 explicitly:

```shell
dotnet run --project samples/Pulse.Mqtt.WorkerSample -- --Mqtt:ProtocolVersion V311
```

The sample publishes five telemetry messages, receives them through a route source block,
processes them through bounded Dataflow stages, logs the negotiated protocol capabilities, and
then stops the host cleanly.
