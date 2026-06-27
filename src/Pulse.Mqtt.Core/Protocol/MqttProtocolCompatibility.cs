namespace Pulse.Mqtt.Protocol;

internal static class MqttProtocolCompatibility
{
    public static void ThrowIfMqtt5PropertyUsedWithV311(
        MqttProtocolVersion protocolVersion,
        bool isSet,
        string packetName,
        string propertyName)
    {
        if (protocolVersion != MqttProtocolVersion.V311 || !isSet)
        {
            return;
        }

        throw new ArgumentException(
            $"MQTT 5-only property '{propertyName}' cannot be used on {packetName} when ProtocolVersion is V311. Use MQTT 5.0 or remove the property.");
    }
}
