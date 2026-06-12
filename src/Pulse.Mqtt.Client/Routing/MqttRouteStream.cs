using System.Threading.Channels;

namespace Pulse.Mqtt.Client;

/// <summary>A consumable stream of routed messages. Disposing removes the route.</summary>
public sealed class MqttRouteStream : IAsyncDisposable
{
    private readonly IDisposable _registration;

    internal MqttRouteStream(ChannelReader<MqttRoutedMessage> reader, IDisposable registration)
    {
        Reader = reader;
        _registration = registration;
    }

    /// <summary>The routed messages, in arrival order.</summary>
    public ChannelReader<MqttRoutedMessage> Reader { get; }

    /// <summary>Streams the routed messages for <c>await foreach</c> consumption.</summary>
    public IAsyncEnumerable<MqttRoutedMessage> ReadAllAsync(CancellationToken cancellationToken = default) =>
        Reader.ReadAllAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _registration.Dispose();
        return ValueTask.CompletedTask;
    }
}
