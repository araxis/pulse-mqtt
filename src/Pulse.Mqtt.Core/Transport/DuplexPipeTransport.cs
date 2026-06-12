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
    public ValueTask DisposeAsync()
    {
        Input.Complete();
        Output.Complete();
        return ValueTask.CompletedTask;
    }
}
