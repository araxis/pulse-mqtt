namespace Pulse.Mqtt.Codec;

/// <summary>Metadata describing how each MQTT 5 property is encoded.</summary>
public static class MqttPropertyTypes
{
    /// <summary>Returns the wire value type for <paramref name="id"/>.</summary>
    /// <exception cref="MqttProtocolException"><paramref name="id"/> is not a defined MQTT 5 property.</exception>
    public static MqttPropertyType GetValueType(MqttPropertyId id) => id switch
    {
        MqttPropertyId.PayloadFormatIndicator => MqttPropertyType.Byte,
        MqttPropertyId.MessageExpiryInterval => MqttPropertyType.FourByteInteger,
        MqttPropertyId.ContentType => MqttPropertyType.Utf8String,
        MqttPropertyId.ResponseTopic => MqttPropertyType.Utf8String,
        MqttPropertyId.CorrelationData => MqttPropertyType.BinaryData,
        MqttPropertyId.SubscriptionIdentifier => MqttPropertyType.VariableByteInteger,
        MqttPropertyId.SessionExpiryInterval => MqttPropertyType.FourByteInteger,
        MqttPropertyId.AssignedClientIdentifier => MqttPropertyType.Utf8String,
        MqttPropertyId.ServerKeepAlive => MqttPropertyType.TwoByteInteger,
        MqttPropertyId.AuthenticationMethod => MqttPropertyType.Utf8String,
        MqttPropertyId.AuthenticationData => MqttPropertyType.BinaryData,
        MqttPropertyId.RequestProblemInformation => MqttPropertyType.Byte,
        MqttPropertyId.WillDelayInterval => MqttPropertyType.FourByteInteger,
        MqttPropertyId.RequestResponseInformation => MqttPropertyType.Byte,
        MqttPropertyId.ResponseInformation => MqttPropertyType.Utf8String,
        MqttPropertyId.ServerReference => MqttPropertyType.Utf8String,
        MqttPropertyId.ReasonString => MqttPropertyType.Utf8String,
        MqttPropertyId.ReceiveMaximum => MqttPropertyType.TwoByteInteger,
        MqttPropertyId.TopicAliasMaximum => MqttPropertyType.TwoByteInteger,
        MqttPropertyId.TopicAlias => MqttPropertyType.TwoByteInteger,
        MqttPropertyId.MaximumQoS => MqttPropertyType.Byte,
        MqttPropertyId.RetainAvailable => MqttPropertyType.Byte,
        MqttPropertyId.UserProperty => MqttPropertyType.Utf8StringPair,
        MqttPropertyId.MaximumPacketSize => MqttPropertyType.FourByteInteger,
        MqttPropertyId.WildcardSubscriptionAvailable => MqttPropertyType.Byte,
        MqttPropertyId.SubscriptionIdentifierAvailable => MqttPropertyType.Byte,
        MqttPropertyId.SharedSubscriptionAvailable => MqttPropertyType.Byte,
        _ => throw new MqttProtocolException($"Unknown MQTT property identifier 0x{(byte)id:X2}."),
    };

    /// <summary>Returns whether <paramref name="id"/> may legally appear more than once in a packet.</summary>
    public static bool AllowsMultiple(MqttPropertyId id) =>
        id is MqttPropertyId.UserProperty or MqttPropertyId.SubscriptionIdentifier;
}
