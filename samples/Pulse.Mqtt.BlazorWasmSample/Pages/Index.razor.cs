using System.Text;
using Microsoft.AspNetCore.Components;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.BlazorWasmSample.Pages;

/// <summary>
/// One connection, one subscription, one publish box. The client outlives renders and is torn
/// down with the page; every mutation of the UI state happens on the renderer via InvokeAsync.
/// </summary>
public partial class Index : IAsyncDisposable
{
    private const int KeepLastMessages = 50;

    private ResilientMqttClient? _client;
    private CancellationTokenSource? _lifetime;
    private long _sequence;

    private string BrokerUrl { get; set; } = "ws://localhost:8083/mqtt";

    private string TopicFilter { get; set; } = "pulse/demo/#";

    private string PublishTopic { get; set; } = "pulse/demo/browser";

    private string PublishText { get; set; } = "hello from wasm";

    private ConnectionState State { get; set; } = ConnectionState.Stopped;

    private string? Error { get; set; }

    private bool IsRunning => _client is not null;

    private bool IsConnected => State == ConnectionState.Connected;

    private List<(long Sequence, string Topic, string Text)> Received { get; } = [];

    private async Task ConnectAsync()
    {
        Error = null;
        if (!Uri.TryCreate(BrokerUrl, UriKind.Absolute, out var uri) || uri.Scheme is not ("ws" or "wss"))
        {
            Error = "Enter a ws:// or wss:// broker URL.";
            return;
        }

        var factory = new WebSocketTransportFactory(new WebSocketTransportOptions { Uri = uri });
        var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket
            {
                ClientId = $"wasm-{Guid.NewGuid():N}"[..16],
                KeepAliveSeconds = 30,
            },
        });

        client.StateChanged += OnStateChanged;
        _client = client;
        _lifetime = new CancellationTokenSource();

        try
        {
            await client.ConnectAsync(_lifetime.Token);
            await client.WaitUntilConnectedAsync(TimeSpan.FromSeconds(10), _lifetime.Token);
            await client.SubscribeAsync(
                [new MqttTopicFilter(TopicFilter) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
                _lifetime.Token);
            _ = ReadMessagesAsync(client, _lifetime.Token);
        }
        catch (Exception error)
        {
            Error = $"Could not connect: {error.Message}";
            await DisconnectAsync();
        }
    }

    private async Task PublishAsync()
    {
        if (_client is not { } client || _lifetime is not { } lifetime)
        {
            return;
        }

        Error = null;
        try
        {
            await client.PublishAsync(
                new MqttPublishPacket
                {
                    Topic = PublishTopic,
                    Payload = Encoding.UTF8.GetBytes(PublishText),
                    QualityOfService = MqttQualityOfService.AtLeastOnce,
                },
                lifetime.Token);
        }
        catch (Exception error)
        {
            Error = $"Publish failed: {error.Message}";
        }
    }

    private async Task DisconnectAsync()
    {
        if (_client is not { } client)
        {
            return;
        }

        client.StateChanged -= OnStateChanged;
        _client = null;
        if (_lifetime is { } lifetime)
        {
            await lifetime.CancelAsync();
            lifetime.Dispose();
            _lifetime = null;
        }

        await client.DisposeAsync();
        State = ConnectionState.Stopped;
    }

    public async ValueTask DisposeAsync() => await DisconnectAsync();

    private async Task ReadMessagesAsync(ResilientMqttClient client, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in client.Messages.ReadAllAsync(cancellationToken))
            {
                var entry = (Interlocked.Increment(ref _sequence), message.Topic, Encoding.UTF8.GetString(message.Payload.Span));
                await InvokeAsync(() =>
                {
                    Received.Insert(0, entry);
                    if (Received.Count > KeepLastMessages)
                    {
                        Received.RemoveAt(Received.Count - 1);
                    }

                    StateHasChanged();
                });
            }
        }
        catch (OperationCanceledException)
        {
            // The page disconnected; nothing to surface.
        }
    }

    private void OnStateChanged(ConnectionStateChanged change) =>
        _ = InvokeAsync(() =>
        {
            State = change.Current;
            if (change.Error is { } error && change.Current == ConnectionState.Faulted)
            {
                Error = error.Message;
            }

            StateHasChanged();
        });
}
