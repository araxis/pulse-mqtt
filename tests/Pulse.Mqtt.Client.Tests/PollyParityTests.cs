using Polly;
using Polly.Retry;
using Pulse.Mqtt;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Resilience.Polly;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>Swap parity: the Polly-backed strategy passes the same supervisor scenarios as the default.</summary>
public sealed class PollyParityTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(15);

    private static PollyReconnectStrategy RetryForever() => new(
        new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = int.MaxValue,
                Delay = TimeSpan.Zero,
                // Rejections must escape so the supervisor can classify and fault.
                ShouldHandle = new PredicateBuilder()
                    .Handle<MqttException>(error => error is not MqttConnectRejectedException),
            })
            .Build());

    private static ResilientMqttClientOptions NewOptions() => new()
    {
        Connect = new MqttConnectPacket { ClientId = "polly", KeepAliveSeconds = 0 },
        ReconnectStrategy = RetryForever(),
    };

    [Fact]
    public async Task Polly_strategy_recovers_from_a_drop()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        var broker1 = await factory.NextBrokerAsync(timeout.Token);
        await broker1.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);

        await broker1.DisposeAsync();

        var broker2 = await factory.NextBrokerAsync(timeout.Token);
        await broker2.AcceptConnectionAsync(timeout.Token);
        await WaitForStateAsync(client, ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task Polly_strategy_still_faults_sticky_on_a_terminal_rejection()
    {
        var factory = new SequencedTransportFactory();
        await using var client = new ResilientMqttClient(factory, NewOptions());

        using var timeout = new CancellationTokenSource(SafetyTimeout);
        await client.ConnectAsync(timeout.Token);

        var broker = await factory.NextBrokerAsync(timeout.Token);
        (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttConnectPacket>();
        await broker.SendAsync(new MqttConnAckPacket { ReasonCode = MqttReasonCode.NotAuthorized }, timeout.Token);

        await WaitForStateAsync(client, ConnectionState.Faulted, timeout.Token);
        var handedOut = factory.ConnectionsHandedOut;
        await Task.Delay(150, timeout.Token);
        factory.ConnectionsHandedOut.ShouldBe(handedOut);
    }

    [Fact]
    public async Task Strategy_reports_each_attempt()
    {
        var starts = new List<int>();
        var failures = 2;

        Task ConnectOnce(CancellationToken ct)
        {
            if (failures-- > 0)
            {
                throw new MqttException("transient");
            }

            return Task.CompletedTask;
        }

        await RetryForever().RunAsync(ConnectOnce, new RecordingContext(starts), CancellationToken.None);

        starts.ShouldBe([1, 2, 3]);
    }

    private sealed class RecordingContext(List<int> starts) : IReconnectContext
    {
        public int Attempt { get; private set; }

        public TimeProvider Time => TimeProvider.System;

        public void OnAttemptStarting(int attempt)
        {
            Attempt = attempt;
            starts.Add(attempt);
        }

        public void OnAttemptFailed(int attempt, Exception error)
        {
        }
    }

    private static async Task WaitForStateAsync(ResilientMqttClient client, ConnectionState state, CancellationToken ct)
    {
        while (client.State != state)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Delay(5, ct);
        }
    }
}
