using System.Net.WebSockets;

namespace Pulse.Mqtt.Transport;

/// <summary>Settings for connecting a <see cref="WebSocketTransport"/>.</summary>
public sealed record WebSocketTransportOptions
{
    private readonly Uri _uri = null!;

    /// <summary>The broker endpoint; the scheme must be <c>ws</c> or <c>wss</c>.</summary>
    public required Uri Uri
    {
        get => _uri;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            if (value.Scheme is not ("ws" or "wss"))
            {
                throw new ArgumentException($"The endpoint scheme must be ws or wss, not '{value.Scheme}'.", nameof(value));
            }

            _uri = value;
        }
    }

    /// <summary>The negotiated subprotocol. Brokers expect <c>mqtt</c>, the default.</summary>
    public string SubProtocol { get; init; } = "mqtt";

    /// <summary>Customizes the underlying client options (headers, proxy, certificates).</summary>
    public Action<ClientWebSocketOptions>? ConfigureClient { get; init; }
}
