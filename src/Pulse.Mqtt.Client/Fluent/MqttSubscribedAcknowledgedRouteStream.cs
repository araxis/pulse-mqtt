using System.Threading.Channels;

namespace Pulse.Mqtt.Client;

/// <summary>
/// A subscribed route stream whose messages require explicit acknowledgement. Disposing removes
/// the local route and unsubscribes the broker filter.
/// </summary>
public sealed class MqttSubscribedAcknowledgedRouteStream : IAsyncDisposable
{
    private readonly ResilientMqttClient _client;
    private readonly MqttAcknowledgedRouteStream _stream;
    private int _disposed;

    internal MqttSubscribedAcknowledgedRouteStream(
        ResilientMqttClient client,
        MqttAcknowledgedRouteStream stream,
        MqttTopicFilter topicFilter)
    {
        _client = client;
        _stream = stream;
        TopicFilter = topicFilter;
    }

    /// <summary>The broker subscription filter created for this route.</summary>
    public MqttTopicFilter TopicFilter { get; }

    /// <summary>The routed messages, in arrival order.</summary>
    public ChannelReader<MqttAcknowledgedRoutedMessage> Reader => _stream.Reader;

    /// <summary>Streams the routed messages for <c>await foreach</c> consumption.</summary>
    public IAsyncEnumerable<MqttAcknowledgedRoutedMessage> ReadAllAsync(CancellationToken cancellationToken = default) =>
        Reader.ReadAllAsync(cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _stream.DisposeAsync().ConfigureAwait(false);
        await ResilientMqttClientFluentExtensions.TryUnsubscribeAsync(_client, TopicFilter.Topic)
            .ConfigureAwait(false);
    }
}
