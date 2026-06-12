namespace Pulse.Mqtt.Transport;

/// <summary>
/// Establishes a connected <see cref="IMqttTransport"/>. This is a swap point: TCP/TLS, WebSocket,
/// QUIC, or in-memory loopback all implement it. The resilient client calls this once per
/// connection attempt, so each reconnect produces a fresh transport.
/// </summary>
public interface IMqttTransportFactory
{
    /// <summary>Connects to the peer and returns a ready-to-use transport.</summary>
    /// <param name="cancellationToken">Cancels the connection attempt.</param>
    ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken);
}
