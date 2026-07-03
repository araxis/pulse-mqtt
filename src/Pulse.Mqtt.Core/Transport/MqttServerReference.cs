using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Pulse.Mqtt.Transport;

/// <summary>
/// Parses the MQTT 5 <c>Server Reference</c> property — <c>host</c>, <c>host:port</c>, or a
/// bracketed IPv6 form such as <c>[2001:db8::1]:1884</c>. A space-delimited list is allowed by
/// the specification; the first entry wins.
/// </summary>
public static class MqttServerReference
{
    /// <summary>
    /// Extracts the host and optional port of the first server in <paramref name="reference"/>.
    /// Returns <see langword="false"/> for empty references or unparseable ports.
    /// </summary>
    public static bool TryParse(string? reference, [NotNullWhen(true)] out string? host, out int? port)
    {
        host = null;
        port = null;
        if (string.IsNullOrWhiteSpace(reference))
        {
            return false;
        }

        var first = reference.AsSpan().Trim();
        var separator = first.IndexOfAny(' ', '\t');
        if (separator >= 0)
        {
            first = first[..separator];
        }

        if (first[0] == '[')
        {
            // Bracketed IPv6: [::1] or [::1]:1884.
            var close = first.IndexOf(']');
            if (close <= 1)
            {
                return false;
            }

            host = first[1..close].ToString();
            var rest = first[(close + 1)..];
            if (rest.IsEmpty)
            {
                return true;
            }

            return rest[0] == ':' && TryParsePort(rest[1..], ref port);
        }

        var colon = first.IndexOf(':');
        if (colon < 0)
        {
            host = first.ToString();
            return true;
        }

        if (first[(colon + 1)..].IndexOf(':') >= 0)
        {
            // More than one colon without brackets: a bare IPv6 address, which cannot carry a port.
            host = first.ToString();
            return true;
        }

        if (colon == 0 || !TryParsePort(first[(colon + 1)..], ref port))
        {
            return false;
        }

        host = first[..colon].ToString();
        return true;
    }

    private static bool TryParsePort(ReadOnlySpan<char> text, ref int? port)
    {
        if (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value) || value is < 1 or > 65535)
        {
            return false;
        }

        port = value;
        return true;
    }
}
