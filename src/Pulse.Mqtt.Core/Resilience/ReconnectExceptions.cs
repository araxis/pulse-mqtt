using Pulse.Mqtt.Protocol;

namespace Pulse.Mqtt.Resilience;

/// <summary>Raised when the broker rejects a CONNECT with a non-success CONNACK reason code.</summary>
public sealed class MqttConnectRejectedException : MqttException
{
    /// <summary>Initializes a new instance carrying the broker's <paramref name="reasonCode"/>.</summary>
    public MqttConnectRejectedException(MqttReasonCode reasonCode)
        : this(reasonCode, null, null)
    {
    }

    /// <summary>Initializes a new instance carrying the broker's CONNACK details.</summary>
    public MqttConnectRejectedException(
        MqttReasonCode reasonCode,
        string? reasonString,
        string? serverReference)
        : base($"The broker rejected the connection with reason 0x{(byte)reasonCode:X2} ({reasonCode}).")
    {
        ReasonCode = reasonCode;
        ReasonString = reasonString;
        ServerReference = serverReference;
    }

    /// <summary>The CONNACK reason code the broker returned.</summary>
    public MqttReasonCode ReasonCode { get; }

    /// <summary>The CONNACK reason string the broker returned, if any.</summary>
    public string? ReasonString { get; }

    /// <summary>The CONNACK server reference the broker returned, if any.</summary>
    public string? ServerReference { get; }
}

/// <summary>A connection attempt failed in a way the reconnect policy considers retryable.</summary>
public sealed class TransientMqttConnectException : MqttException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public TransientMqttConnectException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance with a message and the underlying cause.</summary>
    public TransientMqttConnectException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// A connection attempt failed in a way the reconnect policy considers final. The reconnect loop
/// rethrows this so the supervisor faults rather than retrying forever.
/// </summary>
public sealed class TerminalMqttConnectException : MqttException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public TerminalMqttConnectException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance with a message and the underlying cause.</summary>
    public TerminalMqttConnectException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
