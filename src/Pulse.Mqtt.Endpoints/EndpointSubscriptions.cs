using System.Runtime.CompilerServices;
using Pulse.Mqtt.Client;
using Pulse.Mqtt.Routing;

namespace Pulse.Mqtt.Endpoints;

/// <summary>
/// Reference-counts broker subscriptions taken by <c>MapMqtt</c>, per client. Distinct templates
/// can share one topic filter (<c>a/{x}</c> and <c>a/{y}</c> both subscribe <c>a/+</c>), and the
/// broker holds a single subscription for it — so disposing one endpoint must not unsubscribe a
/// filter another endpoint still needs. All subscribe/unsubscribe traffic for a client's
/// endpoints is serialized through one gate so a concurrent map cannot interleave with the
/// unsubscribe of the endpoint being disposed.
/// </summary>
internal sealed class EndpointSubscriptions
{
    private static readonly ConditionalWeakTable<ResilientMqttClient, EndpointSubscriptions> Instances = [];

    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);

    public static EndpointSubscriptions For(ResilientMqttClient client) => Instances.GetOrCreateValue(client);

    /// <summary>
    /// Subscribes the filter and takes a reference on success (a queued offline subscription
    /// counts — it is applied on the next connection). A denial takes no reference.
    /// </summary>
    public async Task SubscribeAsync(ResilientMqttClient client, MqttRouteTemplate route, MqttTopicFilter filter)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            var granted = await client.SubscribeAsync([filter], CancellationToken.None).ConfigureAwait(false);
            if (granted.Count > 0 && (byte)granted[0] >= 0x80)
            {
                throw new MqttException($"The broker denied the subscription for '{route.Template}': {granted[0]}.");
            }

            _counts[filter.Topic] = _counts.TryGetValue(filter.Topic, out var count) ? count + 1 : 1;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Releases one reference on the filter, unsubscribing only when the last endpoint using it
    /// is disposed. A filter with no reference (its subscription was denied) is left alone.
    /// </summary>
    public async Task ReleaseAsync(ResilientMqttClient client, string topicFilter)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_counts.TryGetValue(topicFilter, out var count))
            {
                return;
            }

            if (count > 1)
            {
                _counts[topicFilter] = count - 1;
                return;
            }

            _counts.Remove(topicFilter);
            await client.UnsubscribeAsync([topicFilter], CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }
}
