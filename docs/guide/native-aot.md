# Native AOT

Pulse.Mqtt is verified for Native AOT: every library compiles with **zero trimming and AOT
warnings**, and the repository gates this with a full-stack smoke application published as a
~3 MB self-contained native binary — broker, client, routing, and JSON messaging all running
compiled ahead of time.

## What makes it work

- **No reflection on any path.** Packet codecs are explicit switches; routing parses
  templates, not expression trees; DI uses keyed services, not scanning.
- **Source-generated JSON.** The serializer takes your `JsonSerializerContext`; nothing falls
  back to reflection-based serialization:

  ```csharp
  [JsonSerializable(typeof(TelemetryReading))]
  public sealed partial class AppJsonContext : JsonSerializerContext;

  new JsonMqttSerializer(AppJsonContext.Default)
  ```

- **Source-generated logging.** All log messages are `LoggerMessage` definitions.
- **`IsAotCompatible` on every library**, so analyzer warnings fail the build the moment a
  change would break trimming.

## Publishing your app

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```shell
dotnet publish -c Release -r linux-x64
```

Nothing Pulse-specific to configure. Keep your own payload types in the
`JsonSerializerContext` and the cut is complete.

## The smoke test

[`aot/Pulse.Mqtt.AotSmoke`](https://github.com/araxis/pulse-mqtt/tree/main/aot/Pulse.Mqtt.AotSmoke)
publishes natively and runs the full stack — in-process broker, resilient client, routed
subscription with captured parameters, typed JSON round trip — proving the claim instead of
stating it.
