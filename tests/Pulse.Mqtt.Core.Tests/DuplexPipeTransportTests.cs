using System.IO.Pipelines;
using Pulse.Mqtt.Transport;
using Xunit;

namespace Pulse.Mqtt.Core.Tests;

public sealed class DuplexPipeTransportTests
{
    [Fact]
    public async Task Disposal_tolerates_already_completed_pipes()
    {
        var (client, server) = LoopbackTransport.CreatePair();

        // A torn-down connection often completes the pipes before the transport is disposed;
        // disposal must treat that as normal teardown, not throw.
        client.Output.Complete();
        client.Input.Complete();

        await client.DisposeAsync();
        await server.DisposeAsync();
    }

    [Fact]
    public async Task Disposal_is_safe_to_repeat()
    {
        var (client, server) = LoopbackTransport.CreatePair();

        await client.DisposeAsync();
        await client.DisposeAsync();
        await server.DisposeAsync();
    }

    [Fact]
    public async Task Disposal_tolerates_a_faulted_reader()
    {
        var pipe = new Pipe();
        var transport = new DuplexPipeTransport(pipe.Reader, pipe.Writer);
        pipe.Reader.Complete(new IOException("simulated transport fault"));

        await transport.DisposeAsync();
    }
}
