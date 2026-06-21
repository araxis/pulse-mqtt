using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Testing;

/// <summary>Context passed to a scripted CONNACK factory.</summary>
public sealed class PulseMqttTestBrokerConnectContext
{
    internal PulseMqttTestBrokerConnectContext(
        string clientId,
        MqttProtocolVersion protocolVersion,
        MqttConnectPacket connect,
        bool sessionPresent,
        MqttConnAckPacket defaultConnAck)
    {
        ClientId = clientId;
        ProtocolVersion = protocolVersion;
        Connect = connect;
        SessionPresent = sessionPresent;
        DefaultConnAck = defaultConnAck;
    }

    /// <summary>The client identifier from the CONNECT packet.</summary>
    public string ClientId { get; }

    /// <summary>The protocol version used by the connecting client.</summary>
    public MqttProtocolVersion ProtocolVersion { get; }

    /// <summary>The CONNECT packet received from the client.</summary>
    public MqttConnectPacket Connect { get; }

    /// <summary>Whether the default broker behavior would resume an existing session.</summary>
    public bool SessionPresent { get; }

    /// <summary>The CONNACK the broker would send without scripting.</summary>
    public MqttConnAckPacket DefaultConnAck { get; }
}

/// <summary>Context passed to a scripted SUBACK factory.</summary>
public sealed class PulseMqttTestBrokerSubscribeContext
{
    internal PulseMqttTestBrokerSubscribeContext(
        string clientId,
        MqttProtocolVersion protocolVersion,
        MqttSubscribePacket subscribe,
        MqttSubAckPacket defaultSubAck)
    {
        ClientId = clientId;
        ProtocolVersion = protocolVersion;
        Subscribe = subscribe;
        DefaultSubAck = defaultSubAck;
    }

    /// <summary>The connected client's identifier.</summary>
    public string ClientId { get; }

    /// <summary>The protocol version used by the connected client.</summary>
    public MqttProtocolVersion ProtocolVersion { get; }

    /// <summary>The SUBSCRIBE packet received from the client.</summary>
    public MqttSubscribePacket Subscribe { get; }

    /// <summary>The SUBACK the broker would send without scripting.</summary>
    public MqttSubAckPacket DefaultSubAck { get; }
}

/// <summary>Context passed to a scripted publish-acknowledgement factory.</summary>
public sealed class PulseMqttTestBrokerPublishContext
{
    internal PulseMqttTestBrokerPublishContext(
        string clientId,
        MqttProtocolVersion protocolVersion,
        MqttPublishPacket publish,
        MqttPublishAckPacket defaultAcknowledgement)
    {
        ClientId = clientId;
        ProtocolVersion = protocolVersion;
        Publish = publish;
        DefaultAcknowledgement = defaultAcknowledgement;
    }

    /// <summary>The connected client's identifier.</summary>
    public string ClientId { get; }

    /// <summary>The protocol version used by the connected client.</summary>
    public MqttProtocolVersion ProtocolVersion { get; }

    /// <summary>The PUBLISH packet received from the client.</summary>
    public MqttPublishPacket Publish { get; }

    /// <summary>The first acknowledgement the broker would send without scripting.</summary>
    public MqttPublishAckPacket DefaultAcknowledgement { get; }
}
