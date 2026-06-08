using Pulse.Mqtt;
using Pulse.Mqtt.Protocol;

// Touches the Core surface so trimming/AOT analysis exercises real types.
var connected = Result<MqttProtocolVersion>.Ok(MqttProtocolVersion.V500);
Console.WriteLine($"AOT smoke: success={connected.IsSuccess}, version={connected.Value}");
