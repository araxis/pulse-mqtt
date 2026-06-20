using LiteDB;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Resilience;

namespace Pulse.Mqtt.Storage.LiteDB;

/// <summary>
/// A durable <see cref="ISessionStore"/> over LiteDB. Subscriptions and the in-flight QoS state are
/// persisted so a persistent-session client resumes — re-subscribing and redelivering unfinished
/// exchanges — after a process restart, not just a reconnect.
/// </summary>
public sealed class LiteDbSessionStore : ISessionStore, IAsyncDisposable, IDisposable
{
    private const string SubscriptionsCollection = "subscriptions";
    private const string OutboundCollection = "inflight_outbound";
    private const string InboundCollection = "inflight_inbound";

    private readonly LiteDbStore _store;

    /// <summary>Opens (creating if needed) the session database at <paramref name="connectionString"/>.</summary>
    /// <param name="connectionString">
    /// A LiteDB connection string, or a plain file path (e.g. <c>mqtt-session.db</c>).
    /// </param>
    public LiteDbSessionStore(string connectionString)
    {
        _store = new LiteDbStore(connectionString);
        try
        {
            _store.RunAsync(database =>
            {
                Subscriptions(database).EnsureIndex("_id");
                Outbound(database).EnsureIndex("_id");
                Inbound(database).EnsureIndex("_id");
            }, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        catch
        {
            _store.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask SaveSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        return _store.RunAsync(database =>
        {
            var subscriptions = Subscriptions(database);
            subscriptions.DeleteAll();
            UpsertSubscriptions(subscriptions, topicFilters);
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<MqttTopicFilter>> LoadSubscriptionsAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(database =>
        {
            var filters = Subscriptions(database)
                .Find(Query.All("_id", Query.Ascending))
                .Select(document => MqttTopicFilter.FromSubscriptionOptions(document["_id"].AsString, (byte)document["Options"].AsInt32))
                .ToList();

            return (IReadOnlyList<MqttTopicFilter>)filters;
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask UpsertSubscriptionsAsync(IReadOnlyList<MqttTopicFilter> topicFilters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topicFilters);
        return _store.RunAsync(database => UpsertSubscriptions(Subscriptions(database), topicFilters), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveSubscriptionsAsync(IReadOnlyList<string> topics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topics);
        return _store.RunAsync(database =>
        {
            var subscriptions = Subscriptions(database);
            foreach (var topic in topics)
            {
                subscriptions.Delete(topic);
            }
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SaveInFlightAsync(MqttInFlightState state, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        return _store.RunAsync(database =>
        {
            var outbound = Outbound(database);
            var inbound = Inbound(database);
            outbound.DeleteAll();
            inbound.DeleteAll();

            long sequence = 1;
            foreach (var entry in state.Outbound)
            {
                outbound.Insert(new BsonDocument
                {
                    ["_id"] = sequence++,
                    ["Stage"] = (int)entry.Stage,
                    ["Version"] = (int)entry.Packet.ProtocolVersion,
                    ["Packet"] = PacketBlob.Encode(entry.Packet),
                });
            }

            foreach (var packetId in state.InboundExactlyOnce)
            {
                inbound.Upsert(new BsonDocument
                {
                    ["_id"] = (int)packetId,
                });
            }
        }, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<MqttInFlightState?> LoadInFlightAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(database =>
        {
            var outbound = Outbound(database)
                .Find(Query.All("_id", Query.Ascending))
                .Select(document =>
                {
                    var stage = (MqttInFlightStage)document["Stage"].AsInt32;
                    var version = (MqttProtocolVersion)document["Version"].AsInt32;
                    return new MqttInFlightPublish(PacketBlob.Decode(document["Packet"].AsBinary, version), stage);
                })
                .ToList();

            var inbound = Inbound(database)
                .Find(Query.All("_id", Query.Ascending))
                .Select(document => (ushort)document["_id"].AsInt32)
                .ToList();

            return outbound.Count == 0 && inbound.Count == 0
                ? null
                : new MqttInFlightState(outbound, inbound);
        }, cancellationToken);

    /// <inheritdoc />
    public ValueTask ClearAsync(CancellationToken cancellationToken) =>
        _store.RunAsync(database =>
        {
            Subscriptions(database).DeleteAll();
            Outbound(database).DeleteAll();
            Inbound(database).DeleteAll();
        }, cancellationToken);

    /// <inheritdoc />
    public void Dispose() => _store.Dispose();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private static ILiteCollection<BsonDocument> Subscriptions(LiteDatabase database) =>
        database.GetCollection<BsonDocument>(SubscriptionsCollection);

    private static ILiteCollection<BsonDocument> Outbound(LiteDatabase database) =>
        database.GetCollection<BsonDocument>(OutboundCollection);

    private static ILiteCollection<BsonDocument> Inbound(LiteDatabase database) =>
        database.GetCollection<BsonDocument>(InboundCollection);

    private static void UpsertSubscriptions(ILiteCollection<BsonDocument> subscriptions, IReadOnlyList<MqttTopicFilter> topicFilters)
    {
        foreach (var filter in topicFilters)
        {
            subscriptions.Upsert(new BsonDocument
            {
                ["_id"] = filter.Topic,
                ["Options"] = (int)filter.ToSubscriptionOptions(),
            });
        }
    }
}
