using System.IO.Pipelines;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// An <see cref="IMqttTransport"/> over a single bidirectional QUIC stream. Create instances
/// through <see cref="QuicTransportFactory"/>.
/// </summary>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
public sealed class QuicTransport : IMqttTransport
{
    private readonly QuicConnection _connection;
    private readonly QuicStream _stream;

    internal QuicTransport(QuicConnection connection, QuicStream stream)
    {
        _connection = connection;
        _stream = stream;

        // leaveOpen: this transport owns the stream and disposes it exactly once below. Without
        // it, completing the reader would dispose the stream and the writer's final flush would
        // then hit a disposed QuicStream.
        Input = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        Output = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    /// <inheritdoc />
    public PipeReader Input { get; }

    /// <inheritdoc />
    public PipeWriter Output { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Teardown path: completing the pipes is best-effort. A peer that already closed the
        // connection can fault the final flush; an abrupt disposal that races an in-flight flush
        // can corrupt the pipe writer's internal state. Both are expected during a disconnect and
        // not worth surfacing from disposal — the stream and connection are still torn down below.
        try
        {
            await Output.CompleteAsync().ConfigureAwait(false);
            await Input.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or ArgumentException)
        {
        }

        await _stream.DisposeAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>Connects <see cref="QuicTransport"/> instances from <see cref="QuicTransportOptions"/>.</summary>
public sealed class QuicTransportFactory : IMqttTransportFactory
{
    private readonly QuicTransportOptions _options;

    /// <summary>Creates a factory for the given endpoint options.</summary>
    public QuicTransportFactory(QuicTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <summary>
    /// Whether QUIC is available here: it needs OS support (for example Windows 11+ or Linux
    /// with libmsquic) plus the msquic native library. Check this to fall back to TCP or
    /// WebSocket gracefully.
    /// </summary>
    public static bool IsSupported => QuicConnection.IsSupported;

    /// <inheritdoc />
    public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
    {
        if (!QuicConnection.IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC is unavailable: the OS or the msquic native library does not support it. " +
                $"Check {nameof(QuicTransportFactory)}.{nameof(IsSupported)} and fall back to TCP or WebSocket.");
        }

        var connectionOptions = new QuicClientConnectionOptions
        {
            RemoteEndPoint = new DnsEndPoint(_options.Host, _options.Port),
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
            ClientAuthenticationOptions = new SslClientAuthenticationOptions
            {
                ApplicationProtocols = [new SslApplicationProtocol(_options.Alpn)],
                TargetHost = _options.TlsTargetHost ?? _options.Host,
                ClientCertificates = _options.ClientCertificates,
                RemoteCertificateValidationCallback = _options.ServerCertificateValidation,
            },
        };

        // The escape hatch runs last so it can override the structured options above.
        _options.ConfigureConnection?.Invoke(connectionOptions);

        var connection = await QuicConnection.ConnectAsync(connectionOptions, cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = await connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, cancellationToken).ConfigureAwait(false);
            return new QuicTransport(connection, stream);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
