using Microsoft.CodeAnalysis;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Endpoints.Generator.Tests;

public sealed class MapMqttGeneratorTests
{
    private const string Prelude = """
        using System;
        using System.Threading;
        using System.Threading.Tasks;
        using Microsoft.Extensions.Hosting;
        using Pulse.Mqtt.Client;
        using Pulse.Mqtt.Endpoints;

        public sealed record Reading(double Value);

        public interface IDeviceStore
        {
            Task SaveAsync(int id, Reading reading, CancellationToken ct);
        }

        public static class Subject
        {
            public static void Map(ResilientMqttClient client, IHost app, IServiceProvider provider)
            {
                CALL
            }
        }
        """;

    private static (System.Collections.Immutable.ImmutableArray<Diagnostic> Diagnostics, bool Emitted) Run(string call) =>
        GeneratorVerifier.Run(Prelude.Replace("CALL", call));

    [Fact]
    public void Flagship_signature_on_the_client_generates_cleanly()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading",
                (int id, Reading reading, IDeviceStore store, CancellationToken ct) => store.SaveAsync(id, reading, ct),
                services: provider);
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Client_service_parameter_without_a_provider_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading",
                (int id, Reading reading, IDeviceStore store, CancellationToken ct) => store.SaveAsync(id, reading, ct));
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE007");
        diagnostics[0].GetMessage().ShouldContain("store");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Client_without_service_parameters_needs_no_provider()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading",
                (int id, Reading reading, CancellationToken ct) => Task.CompletedTask);
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Host_handlers_flow_app_services_without_an_argument()
    {
        var (diagnostics, emitted) = Run("""
            app.MapMqtt("sensors/{id:int}/reading",
                (int id, Reading reading, IDeviceStore store, CancellationToken ct) => store.SaveAsync(id, reading, ct));
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Static_invocation_form_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            MapMqttDelegateExtensions.MapMqtt(client, "alerts/{level}", (string level) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE008");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Non_constant_template_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            var template = Environment.GetEnvironmentVariable("T") ?? "t";
            client.MapMqtt(template, (Reading reading) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE001");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Unbindable_simple_parameter_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading", (int id, double reading) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE003");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Route_parameter_type_must_match_its_constraint()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading", (long id, Reading reading) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE004");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Unknown_constraint_is_refused_at_compile_time()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:decimal}/reading", (Reading reading) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE005");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Unsupported_return_type_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("alerts/{level}", (string level) => level.Length);
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE006");
        emitted.ShouldBeFalse();
    }
}
