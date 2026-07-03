using System.Text;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;
using Pulse.Mqtt.Storage.Sqlite;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>
/// End-to-end coverage for the durable offline queue: a QoS 1 publish made while offline is
/// persisted to a SQLite store (it has no packet identifier yet — the client assigns one at send
/// time), and on connect the flush sends it to the broker with a valid assigned identifier.
/// </summary>
public sealed class DurableOfflineQueueTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task An_offline_qos1_publish_persists_to_sqlite_and_flushes_on_connect()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        using var database = new TempDbFile();
        var factory = new SequencedTransportFactory();
        await using var store = new SqliteMessageStore(database.Path, new OfflineQueueOptions { Capacity = 100 });
        await using var client = new ResilientMqttClient(factory, new ResilientMqttClientOptions
        {
            Connect = new MqttConnectPacket { ClientId = "durable-e2e", KeepAliveSeconds = 0, CleanStart = false },
            MessageStore = store,
        });

        // Offline (never connected): the publish falls to the durable queue. Before the fix this
        // threw ArgumentException from the store because the packet has no identifier yet.
        var outcome = await client.PublishAsync(
            new MqttPublishPacket { Topic = "orders/1", Payload = Encoding.UTF8.GetBytes("hello"), QualityOfService = MqttQualityOfService.AtLeastOnce },
            timeout.Token);
        outcome.Disposition.ShouldBe(PublishDisposition.Queued);
        store.Count.ShouldBe(1);

        // Connect: the connection-up flush drains the durable queue to the broker.
        await client.ConnectAsync(timeout.Token);
        var broker = await factory.NextBrokerAsync(timeout.Token);
        await broker.AcceptConnectionAsync(timeout.Token);

        var flushed = (await broker.ReadPacketAsync(timeout.Token)).ShouldBeOfTypeOrThrow<MqttPublishPacket>();
        flushed.Topic.ShouldBe("orders/1");
        flushed.QualityOfService.ShouldBe(MqttQualityOfService.AtLeastOnce);
        Encoding.UTF8.GetString(flushed.Payload.Span).ShouldBe("hello");
        flushed.PacketIdentifier.ShouldNotBeNull();
        flushed.PacketIdentifier!.Value.ShouldBeGreaterThan((ushort)0); // a real identifier, assigned at send

        // Acknowledge so the flush completes and removes the head.
        await broker.SendAsync(
            new MqttPublishAckPacket { PacketType = MqttPacketType.PubAck, PacketIdentifier = flushed.PacketIdentifier.Value },
            timeout.Token);

        using var settle = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (store.Count > 0 && !settle.IsCancellationRequested)
        {
            await Task.Delay(10, timeout.Token);
        }

        store.Count.ShouldBe(0, "the acknowledged publish is removed from the durable queue");
    }

    private sealed class TempDbFile : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), $"pulse-durable-{Guid.NewGuid():N}.db");

        public void Dispose()
        {
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try
                {
                    System.IO.File.Delete(Path + suffix);
                }
                catch (IOException)
                {
                }
            }
        }
    }
}
