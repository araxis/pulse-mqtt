using System.IO.Pipelines;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// An <see cref="IMqttTransport"/> over an existing <see cref="PipeReader"/> and
/// <see cref="PipeWriter"/> pair. Completing the reader and writer on dispose signals the peer.
/// </summary>
public sealed class DuplexPipeTransport : IMqttTransport
{
    /// <summary>Creates a transport over the given reader and writer.</summary>
    public DuplexPipeTransport(PipeReader input, PipeWriter output)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        Input = input;
        Output = output;
    }

    /// <inheritdoc />
    public PipeReader Input { get; }

    /// <inheritdoc />
    public PipeWriter Output { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Mirror the other transports: complete asynchronously (a stream-backed writer flushes
        // without blocking) and treat completion faults as part of teardown, not as errors —
        // the pipes may already be faulted or completed when a connection is torn down.
        try
        {
            await Output.CompleteAsync().ConfigureAwait(false);
            await Input.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or ArgumentException)
        {
        }
    }
}
