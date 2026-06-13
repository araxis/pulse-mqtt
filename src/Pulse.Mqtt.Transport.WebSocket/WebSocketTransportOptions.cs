using System.Collections.ObjectModel;
using System.Net;
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

    /// <summary>
    /// An explicit proxy for the opening handshake — for reaching a broker through a corporate or
    /// reverse proxy. When <see langword="null"/> (the default) the platform default proxy applies.
    /// </summary>
    public IWebProxy? Proxy { get; init; }

    /// <summary>
    /// Extra request headers sent on the opening handshake, such as an <c>Authorization</c> bearer
    /// token or a routing header a reverse proxy keys on. Empty by default.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = ReadOnlyDictionary<string, string>.Empty;

    /// <summary>
    /// Customizes the underlying client options directly. Applied last, so it can override
    /// <see cref="Proxy"/> and <see cref="Headers"/> or set anything they do not cover (for
    /// example client certificates).
    /// </summary>
    public Action<ClientWebSocketOptions>? ConfigureClient { get; init; }
}
