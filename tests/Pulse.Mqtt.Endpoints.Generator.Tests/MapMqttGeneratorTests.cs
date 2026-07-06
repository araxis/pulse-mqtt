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

        public sealed record Status(int Code);

        public interface IDeviceStore
        {
            Task SaveAsync(int id, Reading reading, CancellationToken ct);

            Task<Status> GetStatusAsync(int id, Reading reading, CancellationToken ct);
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
    public void Context_binding_with_manual_acknowledgement_options_generates_cleanly()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading",
                async (int id, Reading reading, MqttEndpointContext context) => await context.AcknowledgeAsync(context.CancellationToken),
                options: new MqttEndpointOptions { Acknowledgement = MqttAcknowledgementMode.Manual });
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

    [Fact]
    public void Named_arguments_bind_by_name_not_position()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt(handler: (Reading reading) => { }, template: "alerts/x");
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Named_services_argument_out_of_position_counts_as_provided()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading",
                (int id, Reading reading, IDeviceStore store, CancellationToken ct) => store.SaveAsync(id, reading, ct),
                services: provider, options: null);
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Explicit_null_services_argument_is_still_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading",
                (int id, Reading reading, IDeviceStore store, CancellationToken ct) => store.SaveAsync(id, reading, ct),
                null, null);
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE007");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Type_parameter_payload_is_refused()
    {
        var (diagnostics, emitted) = GeneratorVerifier.Run("""
            using Pulse.Mqtt.Client;
            using Pulse.Mqtt.Endpoints;

            public static class Subject
            {
                public static void Map<T>(ResilientMqttClient client) where T : class =>
                    client.MapMqtt("things/x", (T payload) => { });
            }
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE010");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Private_nested_payload_type_is_refused()
    {
        var (diagnostics, emitted) = GeneratorVerifier.Run("""
            using Pulse.Mqtt.Client;
            using Pulse.Mqtt.Endpoints;

            public static class Subject
            {
                private sealed record Secret(int Value);

                public static void Map(ResilientMqttClient client) =>
                    client.MapMqtt("things/x", (Secret payload) => { });
            }
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE010");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Default_parameter_value_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading", (int id = 7) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE009");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Ref_parameter_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("sensors/{id:int}/reading", (ref int id) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE009");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Templates_the_runtime_rejects_are_compile_errors()
    {
        foreach (var template in new[] { "sensors/#/data", "rooms/a{b}c/x", "rooms/{a{b}/x", "$share/g", "$share/{g}/x" })
        {
            var (diagnostics, emitted) = Run($$"""
                client.MapMqtt("{{template}}", (Reading reading) => { });
                """);

            diagnostics.ShouldHaveSingleItem($"template '{template}'").Id.ShouldBe("PMQE005");
            emitted.ShouldBeFalse();
        }
    }

    [Fact]
    public void Shared_subscription_templates_generate_cleanly()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqtt("$share/workers/jobs/{id:int}", (int id, Reading reading) => { });
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Conditional_access_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client?.MapMqtt("alerts/x", (Reading reading) => { });
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE011");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Request_flagship_shape_generates_cleanly()
    {
        var (diagnostics, emitted) = Run("""
            app.MapMqttRequest("devices/{id:int}/status",
                (int id, Reading reading, IDeviceStore store, CancellationToken ct) => store.GetStatusAsync(id, reading, ct));
            """);

        diagnostics.ShouldBeEmpty();
        emitted.ShouldBeTrue();
    }

    [Fact]
    public void Request_sync_and_valuetask_returns_generate_cleanly()
    {
        var (syncDiagnostics, syncEmitted) = Run("""
            client.MapMqttRequest("queries/x", (Reading reading) => new Status(1));
            """);
        syncDiagnostics.ShouldBeEmpty();
        syncEmitted.ShouldBeTrue();

        var (valueTaskDiagnostics, valueTaskEmitted) = Run("""
            client.MapMqttRequest("queries/x", (Reading reading) => ValueTask.FromResult(new Status(1)));
            """);
        valueTaskDiagnostics.ShouldBeEmpty();
        valueTaskEmitted.ShouldBeTrue();
    }

    [Fact]
    public void Request_handler_without_a_return_value_is_refused()
    {
        var (voidDiagnostics, voidEmitted) = Run("""
            client.MapMqttRequest("queries/x", (Reading reading) => { });
            """);
        voidDiagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE012");
        voidEmitted.ShouldBeFalse();

        var (taskDiagnostics, taskEmitted) = Run("""
            client.MapMqttRequest("queries/x", (Reading reading) => Task.CompletedTask);
            """);
        taskDiagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE012");
        taskEmitted.ShouldBeFalse();
    }

    [Fact]
    public void Request_handler_without_a_request_parameter_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqttRequest("queries/{id:int}", (int id) => new Status(id));
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE013");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Request_service_parameter_without_a_provider_is_refused()
    {
        var (diagnostics, emitted) = Run("""
            client.MapMqttRequest("queries/x", (Reading reading, IDeviceStore store) => new Status(1));
            """);

        diagnostics.ShouldHaveSingleItem().Id.ShouldBe("PMQE007");
        emitted.ShouldBeFalse();
    }

    [Fact]
    public void Diagnostics_refresh_when_a_dependent_type_changes()
    {
        const string Caller = """
            using System.Threading;
            using Pulse.Mqtt.Client;
            using Pulse.Mqtt.Endpoints;

            public static class Subject
            {
                public static void Map(ResilientMqttClient client) =>
                    client.MapMqtt("things/x", (Foo foo, Bar bar) => { });
            }
            """;

        // Foo as a class: Foo binds the payload, Bar becomes a service parameter with no
        // provider on the client call → PMQE007. Foo as a struct: an unbindable simple type
        // → PMQE003. The second run must not replay the first run's cached diagnostic.
        var (first, second) = GeneratorVerifier.RunTwice(
            Caller,
            "public sealed class Foo; public sealed class Bar;",
            "public struct Foo; public sealed class Bar;");

        first.ShouldHaveSingleItem().Id.ShouldBe("PMQE007");
        second.ShouldHaveSingleItem().Id.ShouldBe("PMQE003");
    }
}
