using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Analyzers.Tests;

public sealed class MqttUsageAnalyzerTests
{
    [Fact]
    public async Task PMQ0001_reports_bare_pulse_mqtt_task_call()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class ResilientMqttClient
            {
                public Task PublishAsync() => Task.CompletedTask;
            }

            public sealed class Sample
            {
                public void Run(ResilientMqttClient client)
                {
                    client.PublishAsync();
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0001"]);
    }

    [Fact]
    public async Task PMQ0001_ignores_observed_task_calls()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class ResilientMqttClient
            {
                public Task PublishAsync() => Task.CompletedTask;
                public Task ConnectAsync() => Task.CompletedTask;
            }

            public sealed class Sample
            {
                public async Task RunAsync(ResilientMqttClient client)
                {
                    await client.PublishAsync();
                    var pending = client.ConnectAsync();
                    await Task.WhenAll(client.PublishAsync(), pending);
                    _ = client.PublishAsync();
                }

                public Task ReturnAsync(ResilientMqttClient client) => client.ConnectAsync();
            }
            """);

        diagnostics.ShouldBeEmpty();
    }

    [Fact]
    public async Task PMQ0002_reports_missing_available_cancellation_token()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class ResilientMqttClient
            {
                public Task PublishAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class Sample
            {
                public async Task RunAsync(ResilientMqttClient client, CancellationToken ct)
                {
                    await client.PublishAsync();
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0002"]);
    }

    [Fact]
    public async Task PMQ0002_ignores_calls_without_available_token_or_with_supplied_token()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class ResilientMqttClient
            {
                public Task PublishAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class Sample
            {
                public async Task NoTokenAsync(ResilientMqttClient client)
                {
                    await client.PublishAsync();
                }

                public async Task SuppliedTokenAsync(ResilientMqttClient client, CancellationToken cancellationToken)
                {
                    await client.PublishAsync(cancellationToken);
                }
            }
            """);

        diagnostics.ShouldBeEmpty();
    }

    [Fact]
    public async Task PMQ0002_uses_named_local_cancellation_tokens()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class ResilientMqttClient
            {
                public Task ConnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class Sample
            {
                public async Task RunAsync(ResilientMqttClient client)
                {
                    var token = CancellationToken.None;
                    await client.ConnectAsync();
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0002"]);
    }

    [Fact]
    public async Task PMQ0002_ignores_non_pulse_mqtt_apis()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            namespace Other;

            public sealed class Client
            {
                public Task PublishAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            public sealed class Sample
            {
                public async Task RunAsync(Client client, CancellationToken ct)
                {
                    await client.PublishAsync();
                }
            }
            """);

        diagnostics.ShouldBeEmpty();
    }

    [Fact]
    public async Task PMQ0003_reports_synchronous_dispose_on_known_async_owned_resource()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System;
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class MqttRouteStream : IDisposable, IAsyncDisposable
            {
                public void Dispose()
                {
                }

                public ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }

            public sealed class Sample
            {
                public void Run(MqttRouteStream stream)
                {
                    stream.Dispose();
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0003"]);
    }

    [Fact]
    public async Task PMQ0003_reports_regular_using_on_known_async_owned_resource()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System;
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class MqttSubscribedRoute : IDisposable, IAsyncDisposable
            {
                public void Dispose()
                {
                }

                public ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }

            public sealed class Sample
            {
                public void Run()
                {
                    using var route = new MqttSubscribedRoute();
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0003"]);
    }

    [Fact]
    public async Task PMQ0003_ignores_await_using_dispose_async_and_sync_registration_handles()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            using System;
            using System.Threading.Tasks;

            namespace Pulse.Mqtt.Client;

            public sealed class MqttRouteStream : IDisposable, IAsyncDisposable
            {
                public void Dispose()
                {
                }

                public ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }

            public sealed class RouteRegistration : IDisposable
            {
                public void Dispose()
                {
                }
            }

            public sealed class Sample
            {
                public async Task RunAsync(MqttRouteStream stream)
                {
                    await using var owned = stream;
                    await stream.DisposeAsync();
                    using var registration = new RouteRegistration();
                }
            }
            """);

        diagnostics.ShouldBeEmpty();
    }
}
