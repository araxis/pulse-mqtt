namespace Pulse.Mqtt.Transport;

/// <summary>
/// A transport factory that can retarget itself when a broker redirects the client with an MQTT 5
/// <c>Server Reference</c> (DISCONNECT or CONNACK with <c>UseAnotherServer</c> / <c>ServerMoved</c>).
/// The built-in TCP, WebSocket, and QUIC factories implement it; custom factories opt in to make
/// redirect following work through them.
/// </summary>
public interface IRedirectableTransportFactory : IMqttTransportFactory
{
    /// <summary>
    /// Returns a factory that connects to <paramref name="host"/> instead, keeping every other
    /// setting. A <see langword="null"/> <paramref name="port"/> keeps the current port.
    /// </summary>
    IMqttTransportFactory WithServer(string host, int? port);
}
