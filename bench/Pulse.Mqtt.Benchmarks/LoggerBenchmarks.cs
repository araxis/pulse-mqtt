using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Pulse.Mqtt.Benchmarks;

/// <summary>
/// Logging cost on the hot path, 10,000 calls per operation. The library logs through
/// source-generated <see cref="LoggerMessageAttribute"/> methods; the same pattern is measured
/// here against the null logger, a disabled level, and an enabled no-op sink.
/// </summary>
[MemoryDiagnoser]
public partial class LoggerBenchmarks
{
    private static readonly ILogger Null = NullLogger.Instance;
    private static readonly ILogger Disabled = new SinkLogger(enabled: false);
    private static readonly ILogger Enabled = new SinkLogger(enabled: true);

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "MQTT client {ClientId} lost its connection")]
    private static partial void ConnectionLost(ILogger logger, string clientId);

    [Benchmark]
    public void Log_10000_Messages_NullLogger()
    {
        for (var i = 0; i < 10_000; i++)
        {
            ConnectionLost(Null, "client");
        }
    }

    [Benchmark]
    public void Log_10000_Messages_Disabled_Level()
    {
        for (var i = 0; i < 10_000; i++)
        {
            ConnectionLost(Disabled, "client");
        }
    }

    [Benchmark]
    public void Log_10000_Messages_Enabled_Sink()
    {
        for (var i = 0; i < 10_000; i++)
        {
            ConnectionLost(Enabled, "client");
        }
    }

    private sealed class SinkLogger(bool enabled) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => enabled;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            // Force the message to materialize, like a real sink would.
            _ = formatter(state, exception);
        }
    }
}
