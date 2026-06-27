# Pulse MQTT ASP.NET Core sample

This sample is a small ASP.NET Core Minimal API host using `Pulse.Mqtt.DependencyInjection`.
It demonstrates:

- named client registration through `AddPulseMqttClient("telemetry", ...)`
- keyed DI resolution with `[FromKeyedServices("telemetry")]`
- host-managed MQTT lifecycle
- health checks at `/health`, `/ready`, and `/live`
- typed publishing from HTTP to MQTT
- routed MQTT consumption into an in-memory store
- diagnostics and broker capability snapshots
- safe branching for MQTT 5-only behavior through protocol feature guards and broker capabilities

Run it with the in-process test broker:

```shell
dotnet run --project samples/Pulse.Mqtt.AspNetCoreSample
```

Run it against a real broker:

```shell
dotnet run --project samples/Pulse.Mqtt.AspNetCoreSample -- --Mqtt:Host localhost --Mqtt:Port 1883
```

Use MQTT 3.1.1 explicitly:

```shell
dotnet run --project samples/Pulse.Mqtt.AspNetCoreSample -- --Mqtt:ProtocolVersion V311
```

Try the API:

```shell
curl -X POST http://localhost:5000/api/devices/boiler-1/telemetry -H "Content-Type: application/json" -d "{\"unit\":\"C\",\"value\":21.5}"

curl http://localhost:5000/api/devices/boiler-1/telemetry/latest
curl http://localhost:5000/api/mqtt/diagnostics
curl http://localhost:5000/api/mqtt/capabilities
curl http://localhost:5000/ready
```

The capabilities endpoint returns booleans such as `canUseMqtt5RequestResponse` and
`canUseTopicAliases`, so application code can branch on the negotiated connection instead of
guessing based on broker configuration.
