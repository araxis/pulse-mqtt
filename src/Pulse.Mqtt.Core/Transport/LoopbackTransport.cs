using System.IO.Pipelines;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// Creates a pair of in-memory transports connected back-to-back. Bytes written to one endpoint's
/// <see cref="IMqttTransport.Output"/> appear on the other's <see cref="IMqttTransport.Input"/>.
/// Useful for tests and in-process brokering without a real network.
/// </summary>
public static class LoopbackTransport
{
    /// <summary>Creates two connected endpoints. Bytes flow in both directions.</summary>
    /// <returns>The client and server endpoints of the same in-memory link.</returns>
    public static (IMqttTransport Client, IMqttTransport Server) CreatePair()
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();

        var client = new DuplexPipeTransport(serverToClient.Reader, clientToServer.Writer);
        var server = new DuplexPipeTransport(clientToServer.Reader, serverToClient.Writer);

        return (client, server);
    }
}
