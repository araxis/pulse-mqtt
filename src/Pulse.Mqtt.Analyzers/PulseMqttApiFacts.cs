using Microsoft.CodeAnalysis;

namespace Pulse.Mqtt.Analyzers;

internal static class PulseMqttApiFacts
{
    private static readonly (string MetadataName, string[] PropertyNames)[] Mqtt5OnlyPacketProperties =
    [
        ("Pulse.Mqtt.Packets.MqttConnectPacket",
        [
            "SessionExpiryInterval",
            "ReceiveMaximum",
            "MaximumPacketSize",
            "TopicAliasMaximum",
            "RequestResponseInformation",
            "RequestProblemInformation",
            "AuthenticationMethod",
            "AuthenticationData",
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttPublishPacket",
        [
            "PayloadFormatIndicator",
            "MessageExpiryInterval",
            "TopicAlias",
            "ResponseTopic",
            "CorrelationData",
            "ContentType",
            "SubscriptionIdentifiers",
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttSubscribePacket",
        [
            "SubscriptionIdentifier",
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttUnsubscribePacket",
        [
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttDisconnectPacket",
        [
            "ReasonCode",
            "SessionExpiryInterval",
            "ReasonString",
            "ServerReference",
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttPublishAckPacket",
        [
            "ReasonCode",
            "ReasonString",
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttConnAckPacket",
        [
            "SessionExpiryInterval",
            "ReceiveMaximum",
            "MaximumQoS",
            "RetainAvailable",
            "MaximumPacketSize",
            "AssignedClientIdentifier",
            "TopicAliasMaximum",
            "ReasonString",
            "UserProperties",
            "WildcardSubscriptionAvailable",
            "SubscriptionIdentifiersAvailable",
            "SharedSubscriptionAvailable",
            "ServerKeepAlive",
            "ResponseInformation",
            "ServerReference",
            "AuthenticationMethod",
            "AuthenticationData",
        ]),
        ("Pulse.Mqtt.Packets.MqttSubAckPacket",
        [
            "ReasonString",
            "UserProperties",
        ]),
        ("Pulse.Mqtt.Packets.MqttUnsubAckPacket",
        [
            "ReasonCodes",
            "ReasonString",
            "UserProperties",
        ]),
    ];

    private static readonly string[] AsyncOwnedResourceMetadataNames =
    [
        "Pulse.Mqtt.Client.MqttAcknowledgedRouteStream",
        "Pulse.Mqtt.Client.MqttRouteStream",
        "Pulse.Mqtt.Client.MqttRouter",
        "Pulse.Mqtt.Client.MqttSubscribedRoute",
        "Pulse.Mqtt.Client.ResilientMqttClient",
        "Pulse.Mqtt.Connection.MqttConnection",
        "Pulse.Mqtt.Connection.RawMqttClient",
        "Pulse.Mqtt.Testing.PulseMqttTestBroker",
        "Pulse.Mqtt.Transport.DuplexPipeTransport",
        "Pulse.Mqtt.Transport.TcpTransport",
        "Pulse.Mqtt.Transport.WebSocketTransport",
    ];

    public static bool IsPulseMqttAsyncOperation(IMethodSymbol method) =>
        method.Name.EndsWith("Async", StringComparison.Ordinal)
        && IsTaskLike(method.ReturnType)
        && IsPulseMqttSymbol(method);

    public static bool HasOptionalFinalCancellationToken(IMethodSymbol method)
    {
        if (method.Parameters.Length == 0)
        {
            return false;
        }

        var parameter = method.Parameters[method.Parameters.Length - 1];
        return IsCancellationToken(parameter.Type)
            && (parameter.IsOptional || parameter.HasExplicitDefaultValue);
    }

    public static bool IsKnownAsyncOwnedResource(ITypeSymbol type)
    {
        var original = type.OriginalDefinition;
        foreach (var metadataName in AsyncOwnedResourceMetadataNames)
        {
            if (original.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
                == "global::" + metadataName)
            {
                return true;
            }
        }

        return false;
    }

    public static bool TryGetMqtt5OnlyPacketProperties(ITypeSymbol type, out string[] propertyNames)
    {
        var fullyQualifiedName = type.OriginalDefinition.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        foreach (var entry in Mqtt5OnlyPacketProperties)
        {
            if (fullyQualifiedName == "global::" + entry.MetadataName)
            {
                propertyNames = entry.PropertyNames;
                return true;
            }
        }

        propertyNames = [];
        return false;
    }

    public static bool IsCancellationToken(ITypeSymbol type) =>
        type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
        == "global::System.Threading.CancellationToken";

    private static bool IsPulseMqttSymbol(IMethodSymbol method)
    {
        if (method.ContainingNamespace.ToDisplayString().StartsWith("Pulse.Mqtt", StringComparison.Ordinal))
        {
            return true;
        }

        return method.ReducedFrom?.ContainingNamespace.ToDisplayString()
            .StartsWith("Pulse.Mqtt", StringComparison.Ordinal) == true;
    }

    private static bool IsTaskLike(ITypeSymbol type)
    {
        var original = type.OriginalDefinition.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return original is "global::System.Threading.Tasks.Task"
            or "global::System.Threading.Tasks.Task<TResult>"
            or "global::System.Threading.Tasks.ValueTask"
            or "global::System.Threading.Tasks.ValueTask<TResult>";
    }
}
