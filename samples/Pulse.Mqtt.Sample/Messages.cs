using System.Text.Json.Serialization;

namespace Pulse.Mqtt.Sample;

/// <summary>A periodic sensor reading published by a device.</summary>
public sealed record TelemetryReading(string Unit, double Value, DateTimeOffset Timestamp);

/// <summary>Asks a device for its current status.</summary>
public sealed record StatusRequest(string Caller);

/// <summary>A device's answer to a <see cref="StatusRequest"/>.</summary>
public sealed record StatusReply(string DeviceId, string State, TimeSpan Uptime);

/// <summary>Source-generated serialization for every message the sample exchanges.</summary>
[JsonSerializable(typeof(TelemetryReading))]
[JsonSerializable(typeof(StatusRequest))]
[JsonSerializable(typeof(StatusReply))]
public sealed partial class SampleJsonContext : JsonSerializerContext;
