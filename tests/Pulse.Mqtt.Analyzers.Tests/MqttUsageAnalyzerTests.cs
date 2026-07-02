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

    [Fact]
    public async Task PMQ0004_reports_mqtt5_only_publish_property_on_v311_packet()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            namespace Pulse.Mqtt.Protocol
            {
                public enum MqttProtocolVersion
                {
                    V311 = 4,
                    V500 = 5,
                }
            }

            namespace Pulse.Mqtt.Packets
            {
                using Pulse.Mqtt.Protocol;

                public sealed class MqttPublishPacket
                {
                    public string Topic { get; set; } = "";
                    public MqttProtocolVersion ProtocolVersion { get; set; }
                    public string ContentType { get; set; } = "";
                }

                public sealed class Sample
                {
                    public MqttPublishPacket Create() => new()
                    {
                        Topic = "devices/1",
                        ProtocolVersion = MqttProtocolVersion.V311,
                        ContentType = "application/json",
                    };
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0004"]);
    }

    [Fact]
    public async Task PMQ0004_reports_mqtt5_only_subscribe_property_on_v311_packet()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            namespace Pulse.Mqtt.Protocol
            {
                public enum MqttProtocolVersion
                {
                    V311 = 4,
                    V500 = 5,
                }
            }

            namespace Pulse.Mqtt.Packets
            {
                using Pulse.Mqtt.Protocol;

                public sealed class MqttSubscribePacket
                {
                    public MqttProtocolVersion ProtocolVersion { get; set; }
                    public uint? SubscriptionIdentifier { get; set; }
                }

                public sealed class Sample
                {
                    public MqttSubscribePacket Create() => new()
                    {
                        ProtocolVersion = MqttProtocolVersion.V311,
                        SubscriptionIdentifier = 42,
                    };
                }
            }
            """);

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0004"]);
    }

    [Fact]
    public async Task PMQ0004_ignores_v500_packets_common_properties_and_non_pulse_types()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            """
            namespace Pulse.Mqtt.Protocol
            {
                public enum MqttProtocolVersion
                {
                    V311 = 4,
                    V500 = 5,
                }
            }

            namespace Pulse.Mqtt.Packets
            {
                using Pulse.Mqtt.Protocol;

                public sealed class MqttPublishPacket
                {
                    public string Topic { get; set; } = "";
                    public MqttProtocolVersion ProtocolVersion { get; set; }
                    public string ContentType { get; set; } = "";
                }
            }

            namespace Other
            {
                using Pulse.Mqtt.Protocol;

                public sealed class MqttPublishPacket
                {
                    public MqttProtocolVersion ProtocolVersion { get; set; }
                    public string ContentType { get; set; } = "";
                }

                public sealed class Sample
                {
                    public Pulse.Mqtt.Packets.MqttPublishPacket Mqtt5() => new()
                    {
                        ProtocolVersion = MqttProtocolVersion.V500,
                        ContentType = "application/json",
                    };

                    public Pulse.Mqtt.Packets.MqttPublishPacket Mqtt311Common() => new()
                    {
                        Topic = "devices/1",
                        ProtocolVersion = MqttProtocolVersion.V311,
                    };

                    public MqttPublishPacket OtherType() => new()
                    {
                        ProtocolVersion = MqttProtocolVersion.V311,
                        ContentType = "application/json",
                    };
                }
            }
            """);

        diagnostics.ShouldBeEmpty();
    }

    [Fact]
    public async Task PMQ0005_reports_mqtt5_only_raw_options_on_v311_resilient_options()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            ProtocolClientOptionsSource(
                """
                public sealed class Sample
                {
                    public ResilientMqttClientOptions Create() => new()
                    {
                        Connect = new MqttConnectPacket { ProtocolVersion = MqttProtocolVersion.V311 },
                        Raw = new RawMqttClientOptions
                        {
                            UseOutboundTopicAliases = true,
                            Authenticator = new Authenticator(),
                        },
                    };
                }
                """));

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0005", "PMQ0005"]);
    }

    [Fact]
    public async Task PMQ0005_reports_mqtt5_only_raw_options_on_v311_fluent_builder_chain()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            ProtocolClientOptionsSource(
                """
                public sealed class Sample
                {
                    public void Create()
                    {
                        new PulseMqttClientBuilder()
                            .WithProtocolVersion(MqttProtocolVersion.V311)
                            .WithRawOptions(new RawMqttClientOptions { UseOutboundTopicAliases = true });

                        new PulseMqttClientBuilder()
                            .WithRawOptions(new RawMqttClientOptions { Authenticator = new Authenticator() })
                            .WithConnect(new MqttConnectPacket { ProtocolVersion = MqttProtocolVersion.V311 });
                    }
                }
                """));

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0005", "PMQ0005"]);
    }

    [Fact]
    public async Task PMQ0005_ignores_v500_unknown_protocol_disabled_options_and_non_pulse_types()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            ProtocolClientOptionsSource(
                """
                namespace Other
                {
                    public sealed class RawMqttClientOptions
                    {
                        public bool UseOutboundTopicAliases { get; set; }
                    }
                }

                public sealed class Sample
                {
                    public object[] Create(MqttProtocolVersion version) =>
                    new object[]
                    {
                        new ResilientMqttClientOptions
                        {
                            Connect = new MqttConnectPacket { ProtocolVersion = MqttProtocolVersion.V500 },
                            Raw = new RawMqttClientOptions
                            {
                                UseOutboundTopicAliases = true,
                                Authenticator = new Authenticator(),
                            },
                        },
                        new ResilientMqttClientOptions
                        {
                            Connect = new MqttConnectPacket { ProtocolVersion = version },
                            Raw = new RawMqttClientOptions { UseOutboundTopicAliases = true },
                        },
                        new ResilientMqttClientOptions
                        {
                            Connect = new MqttConnectPacket { ProtocolVersion = MqttProtocolVersion.V311 },
                            Raw = new RawMqttClientOptions
                            {
                                UseOutboundTopicAliases = false,
                                Authenticator = null,
                            },
                        },
                        new PulseMqttClientBuilder()
                            .WithProtocolVersion(MqttProtocolVersion.V500)
                            .WithRawOptions(new RawMqttClientOptions { UseOutboundTopicAliases = true }),
                        new Other.RawMqttClientOptions { UseOutboundTopicAliases = true },
                    };
                }
                """));

        diagnostics.ShouldBeEmpty();
    }

    [Fact]
    public async Task PMQ0005_does_not_duplicate_packet_protocol_diagnostics()
    {
        var diagnostics = await AnalyzerVerifier.GetDiagnosticsAsync(
            ProtocolClientOptionsSource(
                """
                public sealed class Sample
                {
                    public ResilientMqttClientOptions Create() => new()
                    {
                        Connect = new MqttConnectPacket
                        {
                            ProtocolVersion = MqttProtocolVersion.V311,
                            AuthenticationMethod = "SCRAM-SHA-256",
                        },
                        Raw = new RawMqttClientOptions { UseOutboundTopicAliases = true },
                    };
                }
                """));

        diagnostics.Select(diagnostic => diagnostic.Id).ShouldBe(["PMQ0004", "PMQ0005"]);
    }

    private static string ProtocolClientOptionsSource(string sample) =>
        """
        namespace Pulse.Mqtt.Protocol
        {
            public enum MqttProtocolVersion
            {
                V311 = 4,
                V500 = 5,
            }
        }

        namespace Pulse.Mqtt.Packets
        {
            using Pulse.Mqtt.Protocol;

            public sealed class MqttConnectPacket
            {
                public MqttProtocolVersion ProtocolVersion { get; set; }
                public string AuthenticationMethod { get; set; } = "";
            }
        }

        namespace Pulse.Mqtt.Connection
        {
            public interface IMqttAuthenticator
            {
            }

            public sealed class RawMqttClientOptions
            {
                public bool UseOutboundTopicAliases { get; set; }
                public IMqttAuthenticator Authenticator { get; set; }
            }
        }

        namespace Pulse.Mqtt.Client
        {
            using Pulse.Mqtt.Connection;
            using Pulse.Mqtt.Packets;
            using Pulse.Mqtt.Protocol;

            public sealed class ResilientMqttClientOptions
            {
                public MqttConnectPacket Connect { get; set; } = new();
                public RawMqttClientOptions Raw { get; set; } = new();
            }

            public sealed class PulseMqttClientBuilder
            {
                public PulseMqttClientBuilder WithProtocolVersion(MqttProtocolVersion version) => this;
                public PulseMqttClientBuilder WithConnect(MqttConnectPacket connect) => this;
                public PulseMqttClientBuilder WithRawOptions(RawMqttClientOptions options) => this;
            }

            public sealed class Authenticator : IMqttAuthenticator
            {
            }

        """
        + sample
        + """

        }
        """;
}
