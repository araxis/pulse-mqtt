using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Pulse.Mqtt.Transport;

/// <summary>Settings for connecting a <see cref="QuicTransport"/>.</summary>
public sealed record QuicTransportOptions
{
    /// <summary>The broker host name or IP address.</summary>
    public required string Host { get; init; }

    /// <summary>The broker QUIC (UDP) port. EMQX listens on 14567 by default.</summary>
    public int Port { get; init; } = 14567;

    /// <summary>The negotiated ALPN protocol. Brokers expect <c>mqtt</c>, the default.</summary>
    public string Alpn { get; init; } = "mqtt";

    /// <summary>
    /// The host name presented for SNI and certificate validation. Defaults to <see cref="Host"/>
    /// when not set. QUIC always negotiates TLS 1.3; there is no plaintext mode.
    /// </summary>
    public string? TlsTargetHost { get; init; }

    /// <summary>Client certificates for mutual TLS, if required by the broker.</summary>
    public X509CertificateCollection? ClientCertificates { get; init; }

    /// <summary>
    /// How long the QUIC connection may stay silent before the QUIC layer closes it. The default
    /// is <see cref="Timeout.InfiniteTimeSpan"/> — no transport-level timeout, matching TCP, so
    /// the MQTT keep-alive owns liveness. Left unset, msquic would silently close a quiet
    /// connection after its own 30-second default.
    /// </summary>
    public TimeSpan IdleTimeout { get; init; } = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// How often the QUIC layer sends PING frames to keep the connection (and NAT bindings)
    /// alive, independently of the MQTT keep-alive. The default is
    /// <see cref="Timeout.InfiniteTimeSpan"/> — no QUIC-level pings.
    /// </summary>
    public TimeSpan KeepAliveInterval { get; init; } = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// Overrides server certificate validation. Leave unset to use the platform default chain
    /// validation. Supplying a callback that returns <see langword="true"/> unconditionally
    /// disables validation and should only be done in tests.
    /// </summary>
    public RemoteCertificateValidationCallback? ServerCertificateValidation { get; init; }

    /// <summary>
    /// Customizes the underlying connection options directly. Applied last, so it can override
    /// anything the structured options above set — idle timeout, stream limits, or the full
    /// <see cref="QuicClientConnectionOptions.ClientAuthenticationOptions"/>.
    /// </summary>
    public Action<QuicClientConnectionOptions>? ConfigureConnection { get; init; }
}
