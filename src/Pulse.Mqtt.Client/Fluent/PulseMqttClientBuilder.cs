using Microsoft.Extensions.Logging;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Serialization;
using Pulse.Mqtt.Transport;

namespace Pulse.Mqtt.Client;

/// <summary>
/// Fluent construction of a <see cref="ResilientMqttClient"/> for use without dependency
/// injection. Everything is plain method calls over <see cref="ResilientMqttClientOptions"/> —
/// no reflection, fully AOT-safe.
/// </summary>
/// <example>
/// <code>
/// await using var client = await new PulseMqttClientBuilder()
///     .WithTcp("broker.example.com", 8883, useTls: true)
///     .WithClientId("service-1")
///     .WithCredentials("device-42", "secret")
///     .WithKeepAlive(TimeSpan.FromSeconds(30))
///     .WithSerializer(new JsonMqttSerializer(AppJsonContext.Default))
///     .BuildAndStartAsync(ct);
/// </code>
/// </example>
public sealed class PulseMqttClientBuilder
{
    private IMqttTransportFactory? _transportFactory;
    private MqttConnectPacket? _connect;
    private string? _clientId;
    private string? _username;
    private string? _password;
    private TimeSpan? _keepAlive;
    private bool? _cleanStart;
    private MqttProtocolVersion? _protocolVersion;
    private BackoffOptions? _backoff;
    private OfflineQueueOptions? _offlineQueue;
    private RawMqttClientOptions? _raw;
    private IReconnectStrategy? _reconnectStrategy;
    private IReconnectDecision? _reconnectDecision;
    private IConnectionLifecycle? _lifecycle;
    private ISessionStore? _sessionStore;
    private IMessageStore? _messageStore;
    private IMqttSerializer? _serializer;
    private ILogger? _logger;
    private TimeProvider? _timeProvider;

    /// <summary>Connects over TCP, optionally with TLS on the default settings.</summary>
    public PulseMqttClientBuilder WithTcp(string host, int port = 1883, bool useTls = false)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);
        _transportFactory = new TcpTransportFactory(new TcpTransportOptions
        {
            Host = host,
            Port = port,
            UseTls = useTls,
        });
        return this;
    }

    /// <summary>Connects through any transport — WebSocket, the in-process test broker, or your own.</summary>
    public PulseMqttClientBuilder WithTransport(IMqttTransportFactory transportFactory)
    {
        ArgumentNullException.ThrowIfNull(transportFactory);
        _transportFactory = transportFactory;
        return this;
    }

    /// <summary>Sets the client identifier. An empty string requests a server-assigned id.</summary>
    public PulseMqttClientBuilder WithClientId(string clientId)
    {
        ArgumentNullException.ThrowIfNull(clientId);
        _clientId = clientId;
        return this;
    }

    /// <summary>Sets the broker credentials.</summary>
    public PulseMqttClientBuilder WithCredentials(string username, string? password = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(username);
        _username = username;
        _password = password;
        return this;
    }

    /// <summary>Sets the keep-alive interval. <see cref="TimeSpan.Zero"/> disables keep-alive.</summary>
    public PulseMqttClientBuilder WithKeepAlive(TimeSpan interval)
    {
        if (interval < TimeSpan.Zero || interval.TotalSeconds > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(interval), interval, "Keep-alive must be between 0 and 65,535 seconds.");
        }

        _keepAlive = interval;
        return this;
    }

    /// <summary>Disables keep-alive pings.</summary>
    public PulseMqttClientBuilder WithoutKeepAlive() => WithKeepAlive(TimeSpan.Zero);

    /// <summary>Whether to start a clean session. <see langword="false"/> resumes broker-side state.</summary>
    public PulseMqttClientBuilder WithCleanStart(bool cleanStart = true)
    {
        _cleanStart = cleanStart;
        return this;
    }

    /// <summary>Sets the MQTT protocol version. The default is 5.0.</summary>
    public PulseMqttClientBuilder WithProtocolVersion(MqttProtocolVersion version)
    {
        _protocolVersion = version;
        return this;
    }

    /// <summary>
    /// Uses <paramref name="connect"/> as the full CONNECT template — wills, session expiry,
    /// enhanced authentication, everything. Mutually exclusive with the individual identity
    /// methods (<see cref="WithClientId"/>, <see cref="WithCredentials"/>, …).
    /// </summary>
    public PulseMqttClientBuilder WithConnect(MqttConnectPacket connect)
    {
        ArgumentNullException.ThrowIfNull(connect);
        _connect = connect;
        return this;
    }

    /// <summary>Bounds for the default reconnect backoff (exponential with full jitter).</summary>
    public PulseMqttClientBuilder WithBackoff(TimeSpan baseDelay, TimeSpan maxDelay, int? maxAttempts = null)
    {
        _backoff = new BackoffOptions { BaseDelay = baseDelay, MaxDelay = maxDelay, MaxAttempts = maxAttempts };
        return this;
    }

    /// <summary>Bounds and overflow policy for the offline publish queue.</summary>
    public PulseMqttClientBuilder WithOfflineQueue(
        int capacity,
        OverflowPolicy overflow = OverflowPolicy.Block,
        bool includeQos0 = false,
        TimeSpan? publishWaitTimeout = null)
    {
        _offlineQueue = new OfflineQueueOptions
        {
            Capacity = capacity,
            Overflow = overflow,
            IncludeQos0 = includeQos0,
            PublishWaitTimeout = publishWaitTimeout,
        };
        return this;
    }

    /// <summary>Per-connection engine settings (handshake timeouts, inbound queue capacity).</summary>
    public PulseMqttClientBuilder WithRawOptions(RawMqttClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _raw = options;
        return this;
    }

    /// <summary>Swaps the reconnect loop — for example the Polly add-on.</summary>
    public PulseMqttClientBuilder WithReconnectStrategy(IReconnectStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _reconnectStrategy = strategy;
        return this;
    }

    /// <summary>Swaps the retry-or-fault classification.</summary>
    public PulseMqttClientBuilder WithReconnectDecision(IReconnectDecision decision)
    {
        ArgumentNullException.ThrowIfNull(decision);
        _reconnectDecision = decision;
        return this;
    }

    /// <summary>Swaps the connection up/down hooks.</summary>
    public PulseMqttClientBuilder WithLifecycle(IConnectionLifecycle lifecycle)
    {
        ArgumentNullException.ThrowIfNull(lifecycle);
        _lifecycle = lifecycle;
        return this;
    }

    /// <summary>Swaps the durable subscription store.</summary>
    public PulseMqttClientBuilder WithSessionStore(ISessionStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _sessionStore = store;
        return this;
    }

    /// <summary>Swaps the offline publish store.</summary>
    public PulseMqttClientBuilder WithMessageStore(IMessageStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _messageStore = store;
        return this;
    }

    /// <summary>Sets the serializer for typed messaging.</summary>
    public PulseMqttClientBuilder WithSerializer(IMqttSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
        return this;
    }

    /// <summary>Sets the structured log sink.</summary>
    public PulseMqttClientBuilder WithLogger(ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
        return this;
    }

    /// <summary>Sets the clock — pass a fake in tests for instant timeouts and backoff.</summary>
    public PulseMqttClientBuilder WithTimeProvider(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        return this;
    }

    /// <summary>Creates the client. Call <see cref="ResilientMqttClient.StartAsync"/> to connect.</summary>
    /// <exception cref="InvalidOperationException">The configuration is incomplete or contradictory.</exception>
    public ResilientMqttClient Build()
    {
        if (_transportFactory is null)
        {
            throw new InvalidOperationException("Configure a transport: WithTcp(...) or WithTransport(...).");
        }

        var options = new ResilientMqttClientOptions
        {
            Connect = BuildConnect(),
            Raw = _raw ?? new RawMqttClientOptions(),
            OfflineQueue = _offlineQueue ?? new OfflineQueueOptions(),
            Backoff = _backoff ?? new BackoffOptions(),
            ReconnectStrategy = _reconnectStrategy,
            ReconnectDecision = _reconnectDecision,
            Lifecycle = _lifecycle,
            SessionStore = _sessionStore,
            MessageStore = _messageStore,
            Serializer = _serializer,
            Logger = _logger,
        };

        return new ResilientMqttClient(_transportFactory, options, _timeProvider);
    }

    /// <summary>Creates the client and starts it — connection proceeds in the background.</summary>
    public async Task<ResilientMqttClient> BuildAndStartAsync(CancellationToken cancellationToken = default)
    {
        var client = Build();
        try
        {
            await client.StartAsync(cancellationToken).ConfigureAwait(false);
            return client;
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private MqttConnectPacket BuildConnect()
    {
        if (_connect is { } connect)
        {
            if (_clientId is not null || _username is not null || _keepAlive is not null
                || _cleanStart is not null || _protocolVersion is not null)
            {
                throw new InvalidOperationException(
                    "Configure the connection either through WithConnect(...) or through the individual methods, not both.");
            }

            return connect;
        }

        if (_clientId is null)
        {
            throw new InvalidOperationException("Configure an identity: WithClientId(...) or WithConnect(...).");
        }

        return new MqttConnectPacket
        {
            ClientId = _clientId,
            Username = _username,
            Password = _password is { } password ? System.Text.Encoding.UTF8.GetBytes(password) : null,
            KeepAliveSeconds = (ushort)(_keepAlive ?? TimeSpan.FromSeconds(60)).TotalSeconds,
            CleanStart = _cleanStart ?? true,
            ProtocolVersion = _protocolVersion ?? MqttProtocolVersion.V500,
        };
    }
}
