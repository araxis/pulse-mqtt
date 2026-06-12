using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Pulse.Mqtt.Transport;

/// <summary>Settings for connecting a <see cref="TcpTransport"/>.</summary>
public sealed record TcpTransportOptions
{
    /// <summary>The broker host name or IP address.</summary>
    public required string Host { get; init; }

    /// <summary>The broker port. Conventionally 1883 for TCP and 8883 for TLS.</summary>
    public int Port { get; init; } = 1883;

    /// <summary>Whether to negotiate TLS after the socket connects.</summary>
    public bool UseTls { get; init; }

    /// <summary>
    /// The host name presented for SNI and certificate validation. Defaults to <see cref="Host"/>
    /// when not set.
    /// </summary>
    public string? TlsTargetHost { get; init; }

    /// <summary>Client certificates for mutual TLS, if required by the broker.</summary>
    public X509CertificateCollection? ClientCertificates { get; init; }

    /// <summary>
    /// Overrides server certificate validation. Leave unset to use the platform default chain
    /// validation. Supplying a callback that returns <see langword="true"/> unconditionally
    /// disables validation and should only be done in tests.
    /// </summary>
    public RemoteCertificateValidationCallback? ServerCertificateValidation { get; init; }
}
