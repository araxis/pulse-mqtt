namespace Pulse.Mqtt.Connection;

/// <summary>
/// Allocates MQTT packet identifiers (1..65535). Identifiers stay reserved until returned, so an
/// id is never reused while its operation is still in flight.
/// </summary>
public sealed class MqttPacketIdAllocator
{
    private readonly HashSet<ushort> _inUse = [];
    private readonly object _gate = new();
    private ushort _next;

    /// <summary>Reserves the next free identifier.</summary>
    /// <exception cref="MqttException">All 65535 identifiers are in flight.</exception>
    public ushort Rent()
    {
        lock (_gate)
        {
            if (_inUse.Count >= ushort.MaxValue)
            {
                throw new MqttException("All packet identifiers are in flight.");
            }

            do
            {
                _next = (ushort)(_next == ushort.MaxValue ? 1 : _next + 1);
            }
            while (!_inUse.Add(_next));

            return _next;
        }
    }

    /// <summary>Releases an identifier for reuse.</summary>
    public void Return(ushort id)
    {
        lock (_gate)
        {
            _inUse.Remove(id);
        }
    }
}
