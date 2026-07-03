using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Pulse.Mqtt.Endpoints.Generator;

/// <summary>
/// Lowers Minimal-API-style <c>MapMqtt(template, delegate)</c> call sites onto the explicit
/// <c>MqttEndpointContext</c> runtime at compile time, via C# interceptors. Everything is
/// resolved here — route/payload/service/token classification, constraint-typed conversion —
/// so the emitted code is plain calls with no reflection, and a call site that cannot be bound
/// is a compile error rather than a runtime fallback.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class MapMqttGenerator : IIncrementalGenerator
{
    private const string EndpointsNamespace = "Pulse.Mqtt.Endpoints";
    private const string DelegateExtensions = "Pulse.Mqtt.Endpoints.MapMqttDelegateExtensions";

    private static readonly DiagnosticDescriptor TemplateNotConstant = new(
        "PMQE001", "Route template must be a constant",
        "The MapMqtt route template must be a constant string so the endpoint can be generated at compile time",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor HandlerNotLambda = new(
        "PMQE002", "Handler must be a lambda",
        "The MapMqtt handler must be a lambda expression so its parameters can be bound at compile time",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ParameterNotBindable = new(
        "PMQE003", "Handler parameter cannot be bound",
        "Cannot bind handler parameter '{0}': {1}",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor RouteTypeMismatch = new(
        "PMQE004", "Route parameter type mismatch",
        "Handler parameter '{0}' ({1}) does not match route parameter '{{{2}}}' — constrain the template ({{{2}:{3}}}) or change the parameter type",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor TemplateInvalid = new(
        "PMQE005", "Route template is invalid",
        "The MapMqtt route template is invalid: {0}",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ReturnTypeUnsupported = new(
        "PMQE006", "Handler return type unsupported",
        "The MapMqtt handler must return void, Task, or ValueTask; '{0}' is not supported",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ServicesNotPassed = new(
        "PMQE007", "Handler needs a service provider",
        "Handler parameter '{0}' resolves from services, but this MapMqtt call passes no provider and every delivery would throw — pass the services argument, or map on the host",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor StaticFormUnsupported = new(
        "PMQE008", "MapMqtt must be called as an extension method",
        "MapMqtt with a delegate handler cannot be generated for the static invocation form — call it as an extension method (client.MapMqtt(...) or app.MapMqtt(...))",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var sites = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is InvocationExpressionSyntax
                {
                    Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "MapMqtt" },
                    ArgumentList.Arguments.Count: >= 2,
                },
                static (syntaxContext, token) => Analyze(syntaxContext, token))
            .Where(static site => site is not null)
            .Select(static (site, _) => site!);

        context.RegisterSourceOutput(sites.Collect(), static (productionContext, all) => Emit(productionContext, all));
    }

    private sealed record Site(
        string InterceptsData,
        int InterceptsVersion,
        Receiver Receiver,
        string Emission,
        ImmutableArray<Diagnostic> Diagnostics)
    {
        public bool Equals(Site? other) =>
            other is not null
            && InterceptsData == other.InterceptsData
            && InterceptsVersion == other.InterceptsVersion
            && Receiver == other.Receiver
            && Emission == other.Emission;

        public override int GetHashCode() => InterceptsData.GetHashCode();
    }

    private enum Receiver
    {
        Client,
        HostSingle,
        HostNamed,
    }

    private enum Binding
    {
        Route,
        Payload,
        Service,
        Context,
        Message,
        Token,
    }

    private sealed record Parameter(string Name, string Type, Binding Binding, string RouteAccessor);

    private static Site? Analyze(GeneratorSyntaxContext context, System.Threading.CancellationToken token)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        if (context.SemanticModel.GetSymbolInfo(invocation, token).Symbol is not IMethodSymbol method
            || method.ContainingType?.ToDisplayString() != DelegateExtensions)
        {
            return null;
        }

        // The call site binds the reduced (instance-form) extension method, where the receiver is
        // not in Parameters; classify from the original static definition.
        var definition = method.ReducedFrom ?? method;
        var receiver = definition.Parameters[0].Type.Name switch
        {
            "ResilientMqttClient" => Receiver.Client,
            _ => definition.Parameters.Length == 5 ? Receiver.HostNamed : Receiver.HostSingle,
        };

        var diagnostics = ImmutableArray.CreateBuilder<Diagnostic>();

        // Every argument index below assumes the reduced form; the static invocation form shifts
        // them all by the receiver and cannot be intercepted with the emitted signatures, so it
        // is rejected honestly rather than misread.
        if (method.ReducedFrom is null)
        {
            diagnostics.Add(Diagnostic.Create(StaticFormUnsupported, invocation.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        var arguments = invocation.ArgumentList.Arguments;
        var templateArgument = receiver == Receiver.HostNamed ? arguments[1] : arguments[0];
        var handlerArgument = receiver == Receiver.HostNamed ? arguments[2] : arguments[1];

        var templateValue = context.SemanticModel.GetConstantValue(templateArgument.Expression, token);
        if (templateValue.Value is not string template)
        {
            diagnostics.Add(Diagnostic.Create(TemplateNotConstant, templateArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        if (handlerArgument.Expression is not (ParenthesizedLambdaExpressionSyntax or SimpleLambdaExpressionSyntax))
        {
            diagnostics.Add(Diagnostic.Create(HandlerNotLambda, handlerArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        if (context.SemanticModel.GetSymbolInfo(handlerArgument.Expression, token).Symbol is not IMethodSymbol lambda)
        {
            diagnostics.Add(Diagnostic.Create(HandlerNotLambda, handlerArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        if (!TryParseTemplate(template, out var routeParameters, out var parseError))
        {
            diagnostics.Add(Diagnostic.Create(TemplateInvalid, templateArgument.GetLocation(), parseError));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        var returnKind = lambda.ReturnType.ToDisplayString() switch
        {
            "void" => "void",
            "System.Threading.Tasks.Task" => "Task",
            "System.Threading.Tasks.ValueTask" => "ValueTask",
            var other => other,
        };
        if (returnKind is not ("void" or "Task" or "ValueTask"))
        {
            diagnostics.Add(Diagnostic.Create(ReturnTypeUnsupported, handlerArgument.GetLocation(), returnKind));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        var parameters = ClassifyParameters(lambda, routeParameters, handlerArgument, diagnostics);
        if (parameters is null)
        {
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        // On the bare client nothing supplies a container implicitly: a service-bound parameter
        // with the services argument omitted at the call site is a guaranteed throw on the first
        // delivery, so it is refused here instead. Host receivers always flow app.Services.
        if (receiver == Receiver.Client && parameters.Any(p => p.Binding == Binding.Service))
        {
            var servicesProvided = arguments.Count >= 4
                || arguments.Any(a => a.NameColon?.Name.Identifier.ValueText == "services");
            if (!servicesProvided)
            {
                var first = parameters.First(p => p.Binding == Binding.Service);
                diagnostics.Add(Diagnostic.Create(ServicesNotPassed, handlerArgument.GetLocation(), first.Name));
                return Fail(context, invocation, receiver, diagnostics, token);
            }
        }

        var location = context.SemanticModel.GetInterceptableLocation(invocation, token);
        if (location is null)
        {
            return null;
        }

        var emission = EmitInterceptorBody(receiver, lambda, parameters, returnKind);
        return new Site(location.Data, location.Version, receiver, emission, diagnostics.ToImmutable());
    }

    private static Site? Fail(
        GeneratorSyntaxContext context,
        InvocationExpressionSyntax invocation,
        Receiver receiver,
        ImmutableArray<Diagnostic>.Builder diagnostics,
        System.Threading.CancellationToken token)
    {
        var location = context.SemanticModel.GetInterceptableLocation(invocation, token);
        return location is null
            ? null
            : new Site(location.Data, location.Version, receiver, Emission: string.Empty, diagnostics.ToImmutable());
    }

    private static List<Parameter>? ClassifyParameters(
        IMethodSymbol lambda,
        Dictionary<string, string> routeParameters,
        ArgumentSyntax handlerArgument,
        ImmutableArray<Diagnostic>.Builder diagnostics)
    {
        var result = new List<Parameter>();
        var payloadSeen = false;

        foreach (var parameter in lambda.Parameters)
        {
            var type = parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var attributes = parameter.GetAttributes();
            var fromRoute = attributes.FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == $"{EndpointsNamespace}.FromRouteAttribute");
            var fromPayload = attributes.Any(a => a.AttributeClass?.ToDisplayString() == $"{EndpointsNamespace}.FromPayloadAttribute");
            var fromServices = attributes.Any(a => a.AttributeClass?.ToDisplayString() == $"{EndpointsNamespace}.FromServicesAttribute");

            if (fromServices)
            {
                result.Add(new Parameter(parameter.Name, type, Binding.Service, string.Empty));
                continue;
            }

            if (fromPayload)
            {
                if (payloadSeen)
                {
                    diagnostics.Add(Diagnostic.Create(ParameterNotBindable, handlerArgument.GetLocation(), parameter.Name, "only one parameter can bind the payload"));
                    return null;
                }

                payloadSeen = true;
                result.Add(new Parameter(parameter.Name, type, Binding.Payload, string.Empty));
                continue;
            }

            var routeName = (fromRoute?.NamedArguments.FirstOrDefault(n => n.Key == "Name").Value.Value as string) ?? parameter.Name;
            if (fromRoute is not null || routeParameters.ContainsKey(routeName))
            {
                if (!routeParameters.TryGetValue(routeName, out var constraint))
                {
                    diagnostics.Add(Diagnostic.Create(ParameterNotBindable, handlerArgument.GetLocation(), parameter.Name, $"the template has no route parameter named '{routeName}'"));
                    return null;
                }

                var accessor = RouteAccessorFor(parameter.Type, constraint, routeName);
                if (accessor is null)
                {
                    diagnostics.Add(Diagnostic.Create(
                        RouteTypeMismatch, handlerArgument.GetLocation(),
                        parameter.Name, parameter.Type.ToDisplayString(), routeName, ConstraintFor(parameter.Type) ?? "?"));
                    return null;
                }

                result.Add(new Parameter(parameter.Name, type, Binding.Route, accessor));
                continue;
            }

            switch (parameter.Type.ToDisplayString())
            {
                case "System.Threading.CancellationToken":
                    result.Add(new Parameter(parameter.Name, type, Binding.Token, string.Empty));
                    continue;
                case $"{EndpointsNamespace}.MqttEndpointContext":
                    result.Add(new Parameter(parameter.Name, type, Binding.Context, string.Empty));
                    continue;
                case "Pulse.Mqtt.Packets.MqttPublishPacket":
                    result.Add(new Parameter(parameter.Name, type, Binding.Message, string.Empty));
                    continue;
            }

            if (parameter.Type.IsValueType || parameter.Type.SpecialType == SpecialType.System_String)
            {
                diagnostics.Add(Diagnostic.Create(
                    ParameterNotBindable, handlerArgument.GetLocation(), parameter.Name,
                    "simple types bind only to route parameters; no route parameter has this name"));
                return null;
            }

            // The first unclaimed complex type is the payload; later ones resolve from services.
            if (!payloadSeen)
            {
                payloadSeen = true;
                result.Add(new Parameter(parameter.Name, type, Binding.Payload, string.Empty));
            }
            else
            {
                result.Add(new Parameter(parameter.Name, type, Binding.Service, string.Empty));
            }
        }

        return result;
    }

    private static string? RouteAccessorFor(ITypeSymbol type, string constraint, string routeName)
    {
        var accessor = type.ToDisplayString() switch
        {
            "string" => "GetString",
            "int" => "GetInt",
            "long" => "GetLong",
            "System.Guid" => "GetGuid",
            "bool" => "GetBool",
            _ => null,
        };
        if (accessor is null)
        {
            return null;
        }

        // A typed parameter needs the matching constraint so non-conforming topics never match;
        // string accepts anything.
        var required = ConstraintFor(type);
        return required is null || required == constraint ? $"{accessor}(\"{routeName}\")" : null;
    }

    private static string? ConstraintFor(ITypeSymbol type) => type.ToDisplayString() switch
    {
        "int" => "int",
        "long" => "long",
        "System.Guid" => "guid",
        "bool" => "bool",
        _ => null,
    };

    private static bool TryParseTemplate(string template, out Dictionary<string, string> parameters, out string error)
    {
        parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        error = string.Empty;
        foreach (var segment in template.Split('/'))
        {
            if (segment.Length > 2 && segment[0] == '{' && segment[segment.Length - 1] == '}')
            {
                var body = segment.Substring(1, segment.Length - 2);
                var colon = body.IndexOf(':');
                var name = colon >= 0 ? body.Substring(0, colon) : body;
                var constraint = colon >= 0 ? body.Substring(colon + 1) : "";
                if (name.Length == 0 || (colon >= 0 && constraint is not ("int" or "long" or "guid" or "bool")))
                {
                    error = $"segment '{segment}' is malformed or uses an unknown constraint (supported: int, long, guid, bool)";
                    return false;
                }

                if (parameters.ContainsKey(name))
                {
                    error = $"duplicate route parameter '{name}'";
                    return false;
                }

                parameters.Add(name, constraint);
            }
        }

        return true;
    }

    private static string EmitInterceptorBody(Receiver receiver, IMethodSymbol lambda, List<Parameter> parameters, string returnKind)
    {
        var payload = parameters.FirstOrDefault(p => p.Binding == Binding.Payload);
        var delegateType = DelegateTypeFor(lambda, parameters, returnKind);

        var arguments = string.Join(", ", parameters.Select(p => p.Binding switch
        {
            Binding.Route => $"context.Route.{p.RouteAccessor}",
            Binding.Payload => "payload",
            Binding.Service => $"global::Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<{p.Type}>(context.Services)",
            Binding.Context => "context",
            Binding.Message => "context.Message",
            Binding.Token => "context.CancellationToken",
            _ => throw new InvalidOperationException(),
        }));

        var call = $"typed({arguments})";
        var body = returnKind switch
        {
            "ValueTask" => call,
            "Task" => $"new global::System.Threading.Tasks.ValueTask({call})",
            _ => $"{{ {call}; return default; }}",
        };
        var lambdaText = payload is null
            ? $"context => {body}"
            : $"(payload, context) => {body}";

        var mapCall = payload is null
            ? $"global::Pulse.Mqtt.Endpoints.PulseMqttEndpointExtensions.MapMqtt(client, template, {lambdaText}, options, services)"
            : $"global::Pulse.Mqtt.Endpoints.PulseMqttEndpointExtensions.MapMqtt<{payload.Type}>(client, template, {lambdaText}, options, services)";

        var resolve = receiver switch
        {
            Receiver.Client => string.Empty,
            Receiver.HostSingle => "        var client = global::Pulse.Mqtt.Endpoints.GeneratedEndpointSupport.ResolveSingleClient(app);\n        var services = (global::System.IServiceProvider?)app.Services;\n",
            Receiver.HostNamed => "        var client = global::Pulse.Mqtt.Endpoints.GeneratedEndpointSupport.ResolveClient(app, clientName);\n        var services = (global::System.IServiceProvider?)app.Services;\n",
            _ => throw new InvalidOperationException(),
        };

        return $"        var typed = ({delegateType})handler;\n{resolve}        return {mapCall};";
    }

    private static string DelegateTypeFor(IMethodSymbol lambda, List<Parameter> parameters, string returnKind)
    {
        var types = parameters.Select(p => p.Type).ToList();
        if (returnKind == "void")
        {
            return types.Count == 0
                ? "global::System.Action"
                : $"global::System.Action<{string.Join(", ", types)}>";
        }

        var returnType = returnKind == "Task"
            ? "global::System.Threading.Tasks.Task"
            : "global::System.Threading.Tasks.ValueTask";
        types.Add(returnType);
        return $"global::System.Func<{string.Join(", ", types)}>";
    }

    private static void Emit(SourceProductionContext context, ImmutableArray<Site> sites)
    {
        foreach (var diagnostic in sites.SelectMany(site => site.Diagnostics))
        {
            context.ReportDiagnostic(diagnostic);
        }

        var valid = sites.Where(site => site.Emission.Length > 0).ToList();
        if (valid.Count == 0)
        {
            return;
        }

        var source = new StringBuilder();
        source.AppendLine("// <auto-generated/>");
        source.AppendLine("#nullable enable");
        source.AppendLine("#pragma warning disable");
        source.AppendLine("namespace System.Runtime.CompilerServices");
        source.AppendLine("{");
        source.AppendLine("    [global::System.AttributeUsage(global::System.AttributeTargets.Method, AllowMultiple = true)]");
        source.AppendLine("    file sealed class InterceptsLocationAttribute : global::System.Attribute");
        source.AppendLine("    {");
        source.AppendLine("        public InterceptsLocationAttribute(int version, string data) { }");
        source.AppendLine("    }");
        source.AppendLine("}");
        source.AppendLine("namespace Pulse.Mqtt.Endpoints.Generated");
        source.AppendLine("{");
        source.AppendLine("    file static class MapMqttInterceptors");
        source.AppendLine("    {");

        var index = 0;
        foreach (var site in valid)
        {
            var signature = site.Receiver switch
            {
                Receiver.Client =>
                    "this global::Pulse.Mqtt.Client.ResilientMqttClient client, string template, global::System.Delegate handler, " +
                    "global::Pulse.Mqtt.Endpoints.MqttEndpointOptions? options = null, global::System.IServiceProvider? services = null",
                Receiver.HostSingle =>
                    "this global::Microsoft.Extensions.Hosting.IHost app, string template, global::System.Delegate handler, " +
                    "global::Pulse.Mqtt.Endpoints.MqttEndpointOptions? options = null",
                Receiver.HostNamed =>
                    "this global::Microsoft.Extensions.Hosting.IHost app, string clientName, string template, global::System.Delegate handler, " +
                    "global::Pulse.Mqtt.Endpoints.MqttEndpointOptions? options = null",
                _ => throw new InvalidOperationException(),
            };

            source.AppendLine($"        [global::System.Runtime.CompilerServices.InterceptsLocation({site.InterceptsVersion}, \"{site.InterceptsData}\")]");
            source.AppendLine($"        public static global::Pulse.Mqtt.Endpoints.MqttEndpoint MapMqtt{index}({signature})");
            source.AppendLine("        {");
            source.AppendLine(site.Emission);
            source.AppendLine("        }");
            index++;
        }

        source.AppendLine("    }");
        source.AppendLine("}");
        context.AddSource("MapMqttInterceptors.g.cs", SourceText.From(source.ToString(), Encoding.UTF8));
    }
}
