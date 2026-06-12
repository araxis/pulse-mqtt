using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// An <see cref="IMqttTransport"/> over a connected TCP socket, optionally wrapped in TLS.
/// Create instances through <see cref="TcpTransportFactory"/>.
/// </summary>
public sealed class TcpTransport : IMqttTransport
{
    private readonly Socket _socket;
    private readonly Stream _stream;

    internal TcpTransport(Socket socket, Stream stream)
    {
        _socket = socket;
        _stream = stream;
        Input = PipeReader.Create(stream);
        Output = PipeWriter.Create(stream);
    }

    /// <inheritdoc />
    public PipeReader Input { get; }

    /// <inheritdoc />
    public PipeWriter Output { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await Input.CompleteAsync().ConfigureAwait(false);
        await Output.CompleteAsync().ConfigureAwait(false);
        await _stream.DisposeAsync().ConfigureAwait(false);
        _socket.Dispose();
    }
}

/// <summary>Connects <see cref="TcpTransport"/> instances from <see cref="TcpTransportOptions"/>.</summary>
public sealed class TcpTransportFactory : IMqttTransportFactory
{
    private readonly TcpTransportOptions _options;

    /// <summary>Creates a factory for the given endpoint options.</summary>
    public TcpTransportFactory(TcpTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(_options.Host, _options.Port, cancellationToken).ConfigureAwait(false);

            Stream stream = new NetworkStream(socket, ownsSocket: false);
            if (_options.UseTls)
            {
                var sslStream = new SslStream(stream, leaveInnerStreamOpen: false, _options.ServerCertificateValidation);
                try
                {
                    await sslStream.AuthenticateAsClientAsync(
                        new SslClientAuthenticationOptions
                        {
                            TargetHost = _options.TlsTargetHost ?? _options.Host,
                            ClientCertificates = _options.ClientCertificates,
                        },
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    await sslStream.DisposeAsync().ConfigureAwait(false);
                    throw;
                }

                stream = sslStream;
            }

            return new TcpTransport(socket, stream);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
