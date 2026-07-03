using System.Buffers;
using System.IO.Pipelines;
using System.Net.WebSockets;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// An <see cref="IMqttTransport"/> over a connected WebSocket, exchanging MQTT bytes as binary
/// frames. Create instances through <see cref="WebSocketTransportFactory"/>.
/// </summary>
public sealed class WebSocketTransport : IMqttTransport
{
    private static readonly TimeSpan GracefulCloseTimeout = TimeSpan.FromSeconds(2);

    private readonly WebSocket _socket;
    private readonly Pipe _input = new();
    private readonly Pipe _output = new();
    private readonly CancellationTokenSource _lifetime = new();
    private readonly Task _receivePump;
    private readonly Task _sendPump;
    private volatile bool _disposed;

    internal WebSocketTransport(WebSocket socket)
    {
        _socket = socket;
        _receivePump = Task.Run(() => ReceivePumpAsync(_lifetime.Token), CancellationToken.None);
        _sendPump = Task.Run(() => SendPumpAsync(_lifetime.Token), CancellationToken.None);
    }

    /// <inheritdoc />
    public PipeReader Input => _input.Reader;

    /// <inheritdoc />
    public PipeWriter Output => _output.Writer;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Completing the writer lets the send pump drain whatever was flushed just before disposal
        // — including a graceful MQTT DISCONNECT — and then run the WebSocket close handshake while
        // the socket is still open. Wait for that before cancelling, so those final bytes are not
        // discarded (cancelling first makes the pump's ReadAsync throw before it drains). A dead
        // peer cannot hang disposal: the drain is bounded, after which cancellation forces teardown.
        _output.Writer.Complete();
        try
        {
            await _sendPump.WaitAsync(GracefulCloseTimeout).ConfigureAwait(false);
        }
        catch
        {
            // The graceful drain timed out (a half-open peer whose send window is full can block
            // the close frame) or the pump faulted. Abort forcibly closes the socket, unblocking a
            // send/close stuck against that peer — without it the Task.WhenAll below would wait on
            // the still-stuck pump unbounded, since _socket.Dispose() (which would unblock it) comes
            // after the wait. The graceful-close budget must be a hard bound on disposal.
            _socket.Abort();
        }

        await _lifetime.CancelAsync().ConfigureAwait(false);

        try
        {
            await Task.WhenAll(_receivePump, _sendPump).ConfigureAwait(false);
        }
        catch
        {
            // Pump failures already propagated through the pipes.
        }

        _socket.Dispose();
        _lifetime.Dispose();
    }

    private async Task ReceivePumpAsync(CancellationToken cancellationToken)
    {
        Exception? fault = null;
        try
        {
            while (true)
            {
                var memory = _input.Writer.GetMemory(4096);
                var result = await _socket.ReceiveAsync(memory, cancellationToken).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                _input.Writer.Advance(result.Count);
                var flushed = await _input.Writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (flushed.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
        catch (Exception error)
        {
            fault = error;
        }

        await _input.Writer.CompleteAsync(fault).ConfigureAwait(false);
    }

    private async Task SendPumpAsync(CancellationToken cancellationToken)
    {
        Exception? fault = null;
        try
        {
            while (true)
            {
                var result = await _output.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;

                if (!buffer.IsEmpty)
                {
                    await SendBufferAsync(buffer, cancellationToken).ConfigureAwait(false);
                }

                _output.Reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            if (_socket.State == WebSocketState.Open)
            {
                await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
        catch (Exception error)
        {
            fault = error;
        }

        await _output.Reader.CompleteAsync(fault).ConfigureAwait(false);
    }

    private async ValueTask SendBufferAsync(ReadOnlySequence<byte> buffer, CancellationToken cancellationToken)
    {
        if (buffer.IsSingleSegment)
        {
            await _socket.SendAsync(buffer.First, WebSocketMessageType.Binary, endOfMessage: true, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        var length = (int)buffer.Length;
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            buffer.CopyTo(rented);
            await _socket.SendAsync(rented.AsMemory(0, length), WebSocketMessageType.Binary, endOfMessage: true, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}

/// <summary>Connects <see cref="WebSocketTransport"/> instances from <see cref="WebSocketTransportOptions"/>.</summary>
public sealed class WebSocketTransportFactory : IMqttTransportFactory, IRedirectableTransportFactory
{
    private readonly WebSocketTransportOptions _options;

    /// <summary>Creates a factory for the given endpoint options.</summary>
    public WebSocketTransportFactory(WebSocketTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    /// <remarks>The scheme, path, and every other option carry over; only host and port change.</remarks>
    public IMqttTransportFactory WithServer(string host, int? port)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);
        var uri = new UriBuilder(_options.Uri) { Host = host };
        if (port is { } newPort)
        {
            uri.Port = newPort;
        }

        return new WebSocketTransportFactory(_options with { Uri = uri.Uri });
    }

    /// <inheritdoc />
    public async ValueTask<IMqttTransport> ConnectAsync(CancellationToken cancellationToken)
    {
        var socket = new ClientWebSocket();
        try
        {
            socket.Options.AddSubProtocol(_options.SubProtocol);
            if (_options.Proxy is { } proxy)
            {
                socket.Options.Proxy = proxy;
            }

            foreach (var header in _options.Headers)
            {
                socket.Options.SetRequestHeader(header.Key, header.Value);
            }

            // The escape hatch runs last so it can override the structured options above.
            _options.ConfigureClient?.Invoke(socket.Options);
            await socket.ConnectAsync(_options.Uri, cancellationToken).ConfigureAwait(false);
            return new WebSocketTransport(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
