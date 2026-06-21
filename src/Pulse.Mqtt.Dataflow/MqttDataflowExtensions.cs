using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Dataflow;

/// <summary>Creates bounded Dataflow source blocks from Pulse MQTT client streams.</summary>
public static class MqttDataflowExtensions
{
    /// <summary>
    /// Consumes the client's raw message stream as a bounded source block. Use either this block,
    /// direct <c>Messages</c> reads, or routing for a given client.
    /// </summary>
    public static MqttDataflowSource<MqttPublishPacket> ToMessageSourceBlock(
        this ResilientMqttClient client,
        MqttDataflowSourceOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return new MqttDataflowSource<MqttPublishPacket>(
            ReadAllAsync(client.Messages),
            options,
            ownedResource: null,
            cancellationToken);
    }

    /// <summary>
    /// Opens a local route stream as a bounded source block. Subscribe the matching broker filter
    /// separately with <c>SubscribeAsync</c>.
    /// </summary>
    public static MqttDataflowSource<MqttRoutedMessage> ToRouteSourceBlock(
        this ResilientMqttClient client,
        string template,
        MqttRouteOptions? routeOptions = null,
        MqttDataflowSourceOptions? sourceOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.ToRouteSourceBlock(
            MqttRouteTemplate.Parse(template),
            routeOptions,
            sourceOptions,
            cancellationToken);
    }

    /// <summary>
    /// Opens a local route stream as a bounded source block. Subscribe the matching broker filter
    /// separately with <c>SubscribeAsync</c>.
    /// </summary>
    public static MqttDataflowSource<MqttRoutedMessage> ToRouteSourceBlock(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttRouteOptions? routeOptions = null,
        MqttDataflowSourceOptions? sourceOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(template);

        var stream = client.OpenRouteStream(template, routeOptions);
        return new MqttDataflowSource<MqttRoutedMessage>(
            stream.ReadAllAsync(),
            sourceOptions,
            stream,
            cancellationToken);
    }

    /// <summary>
    /// Opens an acknowledged local route stream as a bounded source block. Consumers remain
    /// responsible for acknowledging or rejecting each message.
    /// </summary>
    public static MqttDataflowSource<MqttAcknowledgedRoutedMessage> ToAcknowledgedRouteSourceBlock(
        this ResilientMqttClient client,
        string template,
        MqttRouteOptions? routeOptions = null,
        MqttDataflowSourceOptions? sourceOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.ToAcknowledgedRouteSourceBlock(
            MqttRouteTemplate.Parse(template),
            routeOptions,
            sourceOptions,
            cancellationToken);
    }

    /// <summary>
    /// Opens an acknowledged local route stream as a bounded source block. Consumers remain
    /// responsible for acknowledging or rejecting each message.
    /// </summary>
    public static MqttDataflowSource<MqttAcknowledgedRoutedMessage> ToAcknowledgedRouteSourceBlock(
        this ResilientMqttClient client,
        MqttRouteTemplate template,
        MqttRouteOptions? routeOptions = null,
        MqttDataflowSourceOptions? sourceOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(template);

        var stream = client.OpenAcknowledgedRouteStream(template, routeOptions);
        return new MqttDataflowSource<MqttAcknowledgedRoutedMessage>(
            stream.ReadAllAsync(),
            sourceOptions,
            stream,
            cancellationToken);
    }

    /// <summary>Streams connection state transitions as a bounded source block.</summary>
    public static MqttDataflowSource<ConnectionStateChanged> ToStateSourceBlock(
        this ResilientMqttClient client,
        MqttDataflowSourceOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        var channel = Channel.CreateBounded<ConnectionStateChanged>(
            new BoundedChannelOptions(MqttDataflowSourceOptions.Validate(options))
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.DropOldest,
            });

        void OnStateChanged(ConnectionStateChanged change) => channel.Writer.TryWrite(change);

        client.StateChanged += OnStateChanged;
        return new MqttDataflowSource<ConnectionStateChanged>(
            ReadAllAsync(channel.Reader),
            options,
            new CallbackAsyncDisposable(() =>
            {
                client.StateChanged -= OnStateChanged;
                channel.Writer.TryComplete();
            }),
            cancellationToken);
    }

    private static async IAsyncEnumerable<T> ReadAllAsync<T>(
        ChannelReader<T> reader,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private sealed class CallbackAsyncDisposable(Action dispose) : IAsyncDisposable
    {
        private int _disposed;

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                dispose();
            }

            return ValueTask.CompletedTask;
        }
    }
}
