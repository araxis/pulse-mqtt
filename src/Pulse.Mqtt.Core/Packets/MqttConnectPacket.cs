using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Packets;

/// <summary>A CONNECT control packet: the client's request to open a session with the broker.</summary>
public sealed record MqttConnectPacket : MqttPacket
{
    /// <summary>The client identifier (may be empty to request a server-assigned id).</summary>
    public required string ClientId { get; init; }

    /// <summary>The MQTT protocol version to negotiate. Defaults to <see cref="MqttProtocolVersion.V500"/>.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>Whether to start a clean session (discard any existing server-side state).</summary>
    public bool CleanStart { get; init; } = true;

    /// <summary>The keep-alive interval, in seconds.</summary>
    public ushort KeepAliveSeconds { get; init; } = 60;

    /// <summary>The user name, if authentication uses one.</summary>
    public string? Username { get; init; }

    /// <summary>The password (binary), if authentication uses one.</summary>
    public ReadOnlyMemory<byte>? Password { get; init; }

    /// <summary>The will message, if the client registers one.</summary>
    public MqttWillMessage? Will { get; init; }

    /// <summary>The MQTT 5 session expiry interval, in seconds, if set.</summary>
    public uint? SessionExpiryInterval { get; init; }

    /// <summary>The MQTT 5 receive maximum (client's inbound inflight limit), if set.</summary>
    public ushort? ReceiveMaximum { get; init; }

    /// <summary>The MQTT 5 maximum packet size the client will accept, if set.</summary>
    public uint? MaximumPacketSize { get; init; }

    /// <summary>The MQTT 5 topic alias maximum the client supports, if set.</summary>
    public ushort? TopicAliasMaximum { get; init; }

    /// <summary>The MQTT 5 request-response-information flag.</summary>
    public bool RequestResponseInformation { get; init; }

    /// <summary>The MQTT 5 request-problem-information flag. Defaults to <see langword="true"/> per the specification.</summary>
    public bool RequestProblemInformation { get; init; } = true;

    /// <summary>The MQTT 5 enhanced authentication method, if used.</summary>
    public string? AuthenticationMethod { get; init; }

    /// <summary>The MQTT 5 enhanced authentication data, if used.</summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; init; }

    /// <summary>The MQTT 5 user properties carried on the CONNECT.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = [];
}
