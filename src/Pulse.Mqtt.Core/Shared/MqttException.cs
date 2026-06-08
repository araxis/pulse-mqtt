namespace Pulse.Mqtt;

/// <summary>Base type for all errors raised by the Pulse MQTT client.</summary>
public class MqttException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="MqttException"/> class.</summary>
    public MqttException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MqttException"/> class with a message.</summary>
    /// <param name="message">The error message.</param>
    public MqttException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MqttException"/> class with a message and inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The underlying cause.</param>
    public MqttException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>Raised when an MQTT packet cannot be decoded from, or is invalid on, the wire.</summary>
public sealed class MqttProtocolException : MqttException
{
    /// <summary>Initializes a new instance of the <see cref="MqttProtocolException"/> class.</summary>
    public MqttProtocolException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MqttProtocolException"/> class with a message.</summary>
    /// <param name="message">The error message.</param>
    public MqttProtocolException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MqttProtocolException"/> class with a message and inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The underlying cause.</param>
    public MqttProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
