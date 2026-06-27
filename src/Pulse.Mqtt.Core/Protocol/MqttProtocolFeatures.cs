namespace Pulse.Mqtt.Protocol;

/// <summary>Helpers for checking whether MQTT protocol-level features are available.</summary>
public static class MqttProtocolFeatures
{
    /// <summary>Returns whether <paramref name="feature"/> is supported by <paramref name="protocolVersion"/>.</summary>
    public static bool IsSupported(MqttProtocolVersion protocolVersion, MqttProtocolFeature feature)
    {
        ValidateFeature(feature);

        return protocolVersion switch
        {
            MqttProtocolVersion.V500 => true,
            MqttProtocolVersion.V311 => false,
            _ => throw new ArgumentOutOfRangeException(
                nameof(protocolVersion),
                protocolVersion,
                "The MQTT protocol version is not recognized."),
        };
    }

    /// <summary>
    /// Throws <see cref="NotSupportedException"/> when <paramref name="feature"/> is unavailable for
    /// <paramref name="protocolVersion"/>.
    /// </summary>
    public static void EnsureSupported(
        MqttProtocolVersion protocolVersion,
        MqttProtocolFeature feature,
        string? operation = null)
    {
        if (IsSupported(protocolVersion, feature))
        {
            return;
        }

        var operationText = string.IsNullOrWhiteSpace(operation)
            ? "This operation"
            : operation;

        throw new NotSupportedException(
            $"{operationText} requires MQTT 5.0 feature '{feature}', but the configured protocol version is {protocolVersion}.");
    }

    private static void ValidateFeature(MqttProtocolFeature feature)
    {
        if (!Enum.IsDefined(feature))
        {
            throw new ArgumentOutOfRangeException(
                nameof(feature),
                feature,
                "The MQTT protocol feature is not recognized.");
        }
    }
}
