namespace Pulse.Mqtt.Connection;

/// <summary>
/// Drives the MQTT 5 enhanced authentication exchange (SCRAM, OAuth-style challenges,
/// Kerberos). This is a swap point: configure one on <see cref="RawMqttClientOptions"/> and the
/// client carries its method on CONNECT, answers every broker AUTH challenge with the data this
/// produces, and supports client-initiated re-authentication on a live connection. With none
/// configured, no AUTH is ever sent — exactly the simple-credentials behavior.
/// </summary>
public interface IMqttAuthenticator
{
    /// <summary>The authentication method name carried on CONNECT (for example <c>SCRAM-SHA-256</c>).</summary>
    string Method { get; }

    /// <summary>
    /// Produces the authentication data for the next step. Called once with
    /// <see langword="null"/> challenge data for the initial CONNECT (and for the AUTH that
    /// initiates a re-authentication), then once per broker challenge with the broker's data.
    /// Return <see langword="null"/> to send the step without data. Throw to abort — the
    /// failure surfaces as a connect failure or a faulted re-authentication.
    /// </summary>
    ValueTask<ReadOnlyMemory<byte>?> NextDataAsync(ReadOnlyMemory<byte>? challenge, CancellationToken cancellationToken);
}
