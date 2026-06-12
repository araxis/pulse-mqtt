# Request and response

MQTT 5 carries a response topic and correlation data on every message, which makes RPC a
first-class pattern. Pulse wires both sides.

## Caller

```csharp
StatusReply reply = await client.RequestAsync<StatusRequest, StatusReply>(
    "devices/boiler-1/status",
    new StatusRequest(caller: "dashboard"),
    cancellationToken: token);
```

The client subscribes once to its private reply filter (`pulse-rpc/<clientId>/{correlation}`),
stamps each request with a response topic and correlation data, and resolves the matching
reply. Timeouts and QoS come from `MqttRequestOptions`.

A raw overload takes and returns `MqttPublishPacket` when payloads are not typed.

## Responder

```csharp
using var responder = await client.OnRequestAsync<StatusRequest, StatusReply>(
    "devices/{deviceId}/status",
    (request, message, token) =>
    {
        var deviceId = message.Values["deviceId"];
        return ValueTask.FromResult(new StatusReply(deviceId, "online"));
    });
```

The responder deserializes each request, runs the handler, and publishes the reply to the
request's response topic with the correlation data echoed — at the request's QoS. Requests
without a response topic are ignored rather than failed, so fire-and-forget publishes to the
same topic stay harmless.

Both sides need a serializer configured for the typed overloads.
