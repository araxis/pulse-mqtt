using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Client;

/// <summary>The broker capabilities negotiated for the client's current successful connection.</summary>
public sealed record MqttBrokerCapabilitiesSnapshot
{
    /// <summary>The protocol version used for the current connection.</summary>
    public MqttProtocolVersion ProtocolVersion { get; private init; }

    /// <summary>Whether the broker resumed an existing session for this client.</summary>
    public bool SessionPresent { get; private init; }

    /// <summary>The client identifier assigned by the broker, when it generated one.</summary>
    public string? AssignedClientIdentifier { get; private init; }

    /// <summary>The raw MQTT 5 receive maximum from CONNACK, when present.</summary>
    public ushort? ReceiveMaximum { get; private init; }

    /// <summary>The effective receive maximum for MQTT 5, or <c>null</c> when not negotiated.</summary>
    public ushort? EffectiveReceiveMaximum { get; private init; }

    /// <summary>The raw MQTT 5 maximum QoS from CONNACK, when present.</summary>
    public MqttQualityOfService? MaximumQoS { get; private init; }

    /// <summary>The effective maximum QoS for this connection.</summary>
    public MqttQualityOfService EffectiveMaximumQoS { get; private init; } = MqttQualityOfService.ExactlyOnce;

    /// <summary>The raw MQTT 5 retain-available flag from CONNACK, when present.</summary>
    public bool? RetainAvailable { get; private init; }

    /// <summary>Whether retained messages are known to be supported.</summary>
    public MqttBrokerFeatureSupport RetainedMessages { get; private init; }

    /// <summary>The raw MQTT 5 maximum packet size from CONNACK, when present.</summary>
    public uint? MaximumPacketSize { get; private init; }

    /// <summary>The raw MQTT 5 topic-alias maximum from CONNACK, when present.</summary>
    public ushort? TopicAliasMaximum { get; private init; }

    /// <summary>The effective topic-alias maximum; zero means topic aliases are unavailable.</summary>
    public ushort EffectiveTopicAliasMaximum { get; private init; }

    /// <summary>Whether topic aliases are supported on this connection.</summary>
    public MqttBrokerFeatureSupport TopicAliases { get; private init; }

    /// <summary>The raw MQTT 5 wildcard-subscription availability flag from CONNACK, when present.</summary>
    public bool? WildcardSubscriptionAvailable { get; private init; }

    /// <summary>Whether wildcard subscriptions are known to be supported.</summary>
    public MqttBrokerFeatureSupport WildcardSubscriptions { get; private init; }

    /// <summary>The raw MQTT 5 subscription-identifier availability flag from CONNACK, when present.</summary>
    public bool? SubscriptionIdentifiersAvailable { get; private init; }

    /// <summary>Whether subscription identifiers are supported on this connection.</summary>
    public MqttBrokerFeatureSupport SubscriptionIdentifiers { get; private init; }

    /// <summary>The raw MQTT 5 shared-subscription availability flag from CONNACK, when present.</summary>
    public bool? SharedSubscriptionAvailable { get; private init; }

    /// <summary>Whether shared subscriptions are known to be supported.</summary>
    public MqttBrokerFeatureSupport SharedSubscriptions { get; private init; }

    /// <summary>The raw MQTT 5 server keep-alive override from CONNACK, when present.</summary>
    public ushort? ServerKeepAlive { get; private init; }

    /// <summary>The effective keep-alive seconds used for this connection.</summary>
    public ushort EffectiveKeepAliveSeconds { get; private init; }

    /// <summary>The MQTT 5 response information supplied by the broker, when present.</summary>
    public string? ResponseInformation { get; private init; }

    /// <summary>The MQTT 5 server reference supplied by the broker, when present.</summary>
    public string? ServerReference { get; private init; }

    /// <summary>The MQTT 5 enhanced-authentication method negotiated by the broker, when present.</summary>
    public string? AuthenticationMethod { get; private init; }

    internal static MqttBrokerCapabilitiesSnapshot From(MqttConnAckPacket connAck, MqttConnectPacket connect)
    {
        var isMqtt5 = connAck.ProtocolVersion == MqttProtocolVersion.V500;
        var effectiveTopicAliasMaximum = isMqtt5 ? connAck.TopicAliasMaximum ?? (ushort)0 : (ushort)0;

        return new MqttBrokerCapabilitiesSnapshot
        {
            ProtocolVersion = connAck.ProtocolVersion,
            SessionPresent = connAck.SessionPresent,
            AssignedClientIdentifier = connAck.AssignedClientIdentifier,
            ReceiveMaximum = connAck.ReceiveMaximum,
            EffectiveReceiveMaximum = isMqtt5 ? connAck.ReceiveMaximum ?? ushort.MaxValue : null,
            MaximumQoS = connAck.MaximumQoS,
            EffectiveMaximumQoS = connAck.MaximumQoS ?? MqttQualityOfService.ExactlyOnce,
            RetainAvailable = connAck.RetainAvailable,
            RetainedMessages = isMqtt5 ? FromDefaultTrue(connAck.RetainAvailable) : MqttBrokerFeatureSupport.Unknown,
            MaximumPacketSize = connAck.MaximumPacketSize,
            TopicAliasMaximum = connAck.TopicAliasMaximum,
            EffectiveTopicAliasMaximum = effectiveTopicAliasMaximum,
            TopicAliases = effectiveTopicAliasMaximum > 0
                ? MqttBrokerFeatureSupport.Supported
                : MqttBrokerFeatureSupport.NotSupported,
            WildcardSubscriptionAvailable = connAck.WildcardSubscriptionAvailable,
            WildcardSubscriptions = isMqtt5
                ? FromDefaultTrue(connAck.WildcardSubscriptionAvailable)
                : MqttBrokerFeatureSupport.Unknown,
            SubscriptionIdentifiersAvailable = connAck.SubscriptionIdentifiersAvailable,
            SubscriptionIdentifiers = isMqtt5
                ? FromDefaultTrue(connAck.SubscriptionIdentifiersAvailable)
                : MqttBrokerFeatureSupport.NotSupported,
            SharedSubscriptionAvailable = connAck.SharedSubscriptionAvailable,
            SharedSubscriptions = isMqtt5
                ? FromDefaultTrue(connAck.SharedSubscriptionAvailable)
                : MqttBrokerFeatureSupport.NotSupported,
            ServerKeepAlive = connAck.ServerKeepAlive,
            EffectiveKeepAliveSeconds = connAck.ServerKeepAlive ?? connect.KeepAliveSeconds,
            ResponseInformation = connAck.ResponseInformation,
            ServerReference = connAck.ServerReference,
            AuthenticationMethod = connAck.AuthenticationMethod,
        };
    }

    private static MqttBrokerFeatureSupport FromDefaultTrue(bool? value) =>
        value == false ? MqttBrokerFeatureSupport.NotSupported : MqttBrokerFeatureSupport.Supported;
}
