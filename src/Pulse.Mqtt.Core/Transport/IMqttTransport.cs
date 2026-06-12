using System.IO.Pipelines;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// A connected, bidirectional byte transport for MQTT. Read inbound bytes from <see cref="Input"/>
/// and write outbound bytes to <see cref="Output"/>. Dispose to close the connection. Obtain a
/// transport from an <see cref="IMqttTransportFactory"/>; to reconnect, request a new one.
/// </summary>
public interface IMqttTransport : IAsyncDisposable
{
    /// <summary>The stream of bytes arriving from the peer.</summary>
    PipeReader Input { get; }

    /// <summary>The stream of bytes to send to the peer.</summary>
    PipeWriter Output { get; }
}
