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
        "The MapMqtt handler must return void, Task, or ValueTask; '{0}' is not supported — to reply with a value, map it with MapMqttRequest",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ServicesNotPassed = new(
        "PMQE007", "Handler needs a service provider",
        "Handler parameter '{0}' resolves from services, but this MapMqtt call passes no provider and every delivery would throw — pass the services argument, or map on the host",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor StaticFormUnsupported = new(
        "PMQE008", "MapMqtt must be called as an extension method",
        "MapMqtt with a delegate handler cannot be generated for the static invocation form — call it as an extension method (client.MapMqtt(...) or app.MapMqtt(...))",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ParameterShapeUnsupported = new(
        "PMQE009", "Handler parameter shape unsupported",
        "Handler parameter '{0}' has {1}, which the generated binding cannot invoke — MapMqtt handler parameters must be plain by-value parameters",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor TypeNotEmittable = new(
        "PMQE010", "Handler parameter type unusable in generated code",
        "Handler parameter type '{0}' cannot be used in generated code: {1}",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ConditionalAccessUnsupported = new(
        "PMQE011", "MapMqtt cannot be conditionally accessed",
        "MapMqtt with a delegate handler cannot be generated behind a conditional access (?.) — call it on a non-null receiver",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor RequestMustReturn = new(
        "PMQE012", "Request handler must return the reply",
        "The MapMqttRequest handler's return value is the reply — it must return TResponse, Task<TResponse>, or ValueTask<TResponse>; '{0}' is not supported. For a handler with no reply, map it with MapMqtt.",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor RequestNeedsPayload = new(
        "PMQE013", "Request handler needs a request parameter",
        "The MapMqttRequest handler must take the deserialized request as a parameter (a complex type, or one marked [FromPayload])",
        "Pulse.Mqtt.Endpoints", DiagnosticSeverity.Error, isEnabledByDefault: true);

    private static readonly Dictionary<string, DiagnosticDescriptor> Descriptors =
        new DiagnosticDescriptor[]
        {
            TemplateNotConstant, HandlerNotLambda, ParameterNotBindable, RouteTypeMismatch,
            TemplateInvalid, ReturnTypeUnsupported, ServicesNotPassed, StaticFormUnsupported,
            ParameterShapeUnsupported, TypeNotEmittable, ConditionalAccessUnsupported,
            RequestMustReturn, RequestNeedsPayload,
        }.ToDictionary(descriptor => descriptor.Id);

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var sites = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is InvocationExpressionSyntax
                {
                    Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "MapMqtt" or "MapMqttRequest" }
                        or MemberBindingExpressionSyntax { Name.Identifier.ValueText: "MapMqtt" or "MapMqttRequest" },
                    ArgumentList.Arguments.Count: >= 2,
                },
                static (syntaxContext, token) => Analyze(syntaxContext, token))
            .Where(static site => site is not null)
            .Select(static (site, _) => site!);

        context.RegisterSourceOutput(sites.Collect(), static (productionContext, all) => Emit(productionContext, all));
    }

    /// <summary>
    /// A diagnostic captured as plain values so <see cref="Site"/> stays fully value-equatable
    /// for incremental caching — holding <see cref="Diagnostic"/> instances would exclude them
    /// from equality (stale diagnostics on replay) and root stale compilations via locations.
    /// </summary>
    private sealed record DiagnosticInfo(
        string Id,
        string FilePath,
        int SpanStart,
        int SpanLength,
        int StartLine,
        int StartCharacter,
        int EndLine,
        int EndCharacter,
        string PackedArguments)
    {
        public static DiagnosticInfo Create(DiagnosticDescriptor descriptor, Location location, params object?[] arguments)
        {
            var lineSpan = location.GetLineSpan();
            return new DiagnosticInfo(
                descriptor.Id,
                location.SourceTree?.FilePath ?? lineSpan.Path ?? string.Empty,
                location.SourceSpan.Start,
                location.SourceSpan.Length,
                lineSpan.StartLinePosition.Line,
                lineSpan.StartLinePosition.Character,
                lineSpan.EndLinePosition.Line,
                lineSpan.EndLinePosition.Character,
                string.Join("\u0001", arguments.Select(argument => argument?.ToString() ?? string.Empty)));
        }

        public Diagnostic ToDiagnostic()
        {
            var location = Location.Create(
                FilePath,
                new TextSpan(SpanStart, SpanLength),
                new LinePositionSpan(
                    new LinePosition(StartLine, StartCharacter),
                    new LinePosition(EndLine, EndCharacter)));
            object[] arguments = PackedArguments.Length == 0 ? [] : PackedArguments.Split('\u0001');
            return Diagnostic.Create(Descriptors[Id], location, arguments);
        }
    }

    private sealed record Site(
        string InterceptsData,
        int InterceptsVersion,
        Receiver Receiver,
        string Emission,
        ImmutableArray<DiagnosticInfo> Diagnostics)
    {
        public bool Equals(Site? other) =>
            other is not null
            && InterceptsData == other.InterceptsData
            && InterceptsVersion == other.InterceptsVersion
            && Receiver == other.Receiver
            && Emission == other.Emission
            && Diagnostics.SequenceEqual(other.Diagnostics);

        public override int GetHashCode() => InterceptsData.GetHashCode() ^ Emission.GetHashCode() ^ Diagnostics.Length;
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

        var diagnostics = ImmutableArray.CreateBuilder<DiagnosticInfo>();

        // client?.MapMqtt(...) cannot be intercepted; without this check it would compile clean
        // and hit the throwing fallback at runtime.
        if (invocation.Expression is MemberBindingExpressionSyntax)
        {
            diagnostics.Add(DiagnosticInfo.Create(ConditionalAccessUnsupported, invocation.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        // Argument positions below assume the reduced form; the static invocation form shifts
        // them all by the receiver and cannot be intercepted with the emitted signatures, so it
        // is rejected honestly rather than misread.
        if (method.ReducedFrom is null)
        {
            diagnostics.Add(DiagnosticInfo.Create(StaticFormUnsupported, invocation.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        var arguments = invocation.ArgumentList.Arguments;
        var templateArgument = FindArgument(arguments, method, "template");
        var handlerArgument = FindArgument(arguments, method, "handler");
        if (templateArgument is null || handlerArgument is null)
        {
            return null;
        }

        var templateValue = context.SemanticModel.GetConstantValue(templateArgument.Expression, token);
        if (templateValue.Value is not string template)
        {
            diagnostics.Add(DiagnosticInfo.Create(TemplateNotConstant, templateArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        if (handlerArgument.Expression is not (ParenthesizedLambdaExpressionSyntax or SimpleLambdaExpressionSyntax))
        {
            diagnostics.Add(DiagnosticInfo.Create(HandlerNotLambda, handlerArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        if (context.SemanticModel.GetSymbolInfo(handlerArgument.Expression, token).Symbol is not IMethodSymbol lambda)
        {
            diagnostics.Add(DiagnosticInfo.Create(HandlerNotLambda, handlerArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        if (!TryParseTemplate(template, out var routeParameters, out var parseError))
        {
            diagnostics.Add(DiagnosticInfo.Create(TemplateInvalid, templateArgument.GetLocation(), parseError));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        var isRequest = definition.Name == "MapMqttRequest";
        string returnKind;
        var responseType = string.Empty;
        if (isRequest)
        {
            // The return value is the reply: TResponse, Task<TResponse>, or ValueTask<TResponse>.
            ITypeSymbol? response = null;
            var wrapper = "Sync";
            if (lambda.ReturnType is INamedTypeSymbol { IsGenericType: true } generic)
            {
                switch (generic.ConstructedFrom.ToDisplayString())
                {
                    case "System.Threading.Tasks.Task<TResult>":
                        wrapper = "Task";
                        response = generic.TypeArguments[0];
                        break;
                    case "System.Threading.Tasks.ValueTask<TResult>":
                        wrapper = "ValueTask";
                        response = generic.TypeArguments[0];
                        break;
                }
            }

            if (response is null)
            {
                var display = lambda.ReturnType.ToDisplayString();
                if (display is "void" or "System.Threading.Tasks.Task" or "System.Threading.Tasks.ValueTask")
                {
                    diagnostics.Add(DiagnosticInfo.Create(RequestMustReturn, handlerArgument.GetLocation(), display));
                    return Fail(context, invocation, receiver, diagnostics, token);
                }

                response = lambda.ReturnType;
            }

            if (!IsEmittable(response, out var responseReason))
            {
                diagnostics.Add(DiagnosticInfo.Create(TypeNotEmittable, handlerArgument.GetLocation(), response.ToDisplayString(), responseReason));
                return Fail(context, invocation, receiver, diagnostics, token);
            }

            returnKind = wrapper;
            responseType = response.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        }
        else
        {
            returnKind = lambda.ReturnType.ToDisplayString() switch
            {
                "void" => "void",
                "System.Threading.Tasks.Task" => "Task",
                "System.Threading.Tasks.ValueTask" => "ValueTask",
                var other => other,
            };
            if (returnKind is not ("void" or "Task" or "ValueTask"))
            {
                diagnostics.Add(DiagnosticInfo.Create(ReturnTypeUnsupported, handlerArgument.GetLocation(), returnKind));
                return Fail(context, invocation, receiver, diagnostics, token);
            }
        }

        var parameters = ClassifyParameters(lambda, routeParameters, handlerArgument, diagnostics);
        if (parameters is null)
        {
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        // A request handler must bind the deserialized request: the runtime deserializes to
        // TRequest before the handler runs, so a call site with no payload parameter has no
        // TRequest to name.
        if (isRequest && !parameters.Any(p => p.Binding == Binding.Payload))
        {
            diagnostics.Add(DiagnosticInfo.Create(RequestNeedsPayload, handlerArgument.GetLocation()));
            return Fail(context, invocation, receiver, diagnostics, token);
        }

        // On the bare client nothing supplies a container implicitly: a service-bound parameter
        // with the services argument omitted (or passed as a null constant) is a guaranteed
        // throw on the first delivery, so it is refused here instead. Host receivers always
        // flow app.Services.
        if (receiver == Receiver.Client && parameters.Any(p => p.Binding == Binding.Service))
        {
            var servicesArgument = FindArgument(arguments, method, "services");
            var servicesProvided = servicesArgument is not null
                && !IsNullConstant(context.SemanticModel, servicesArgument.Expression, token);
            if (!servicesProvided)
            {
                var first = parameters.First(p => p.Binding == Binding.Service);
                diagnostics.Add(DiagnosticInfo.Create(ServicesNotPassed, handlerArgument.GetLocation(), first.Name));
                return Fail(context, invocation, receiver, diagnostics, token);
            }
        }

        var location = context.SemanticModel.GetInterceptableLocation(invocation, token);
        if (location is null)
        {
            return null;
        }

        var emission = isRequest
            ? EmitRequestInterceptorBody(receiver, lambda, parameters, returnKind, responseType)
            : EmitInterceptorBody(receiver, lambda, parameters, returnKind);
        return new Site(location.Data, location.Version, receiver, emission, diagnostics.ToImmutable());
    }

    /// <summary>
    /// Resolves the argument for a parameter by name, honoring named arguments in any order —
    /// positional indexing alone misreads legal call sites such as
    /// <c>MapMqtt(handler: ..., template: ...)</c>.
    /// </summary>
    private static ArgumentSyntax? FindArgument(
        SeparatedSyntaxList<ArgumentSyntax> arguments,
        IMethodSymbol method,
        string parameterName)
    {
        for (var i = 0; i < arguments.Count; i++)
        {
            var argument = arguments[i];
            if (argument.NameColon is { } nameColon)
            {
                if (nameColon.Name.Identifier.ValueText == parameterName)
                {
                    return argument;
                }
            }
            else if (i < method.Parameters.Length && method.Parameters[i].Name == parameterName)
            {
                return argument;
            }
        }

        return null;
    }

    private static bool IsNullConstant(
        SemanticModel semanticModel,
        ExpressionSyntax expression,
        System.Threading.CancellationToken token)
    {
        var constant = semanticModel.GetConstantValue(expression, token);
        return constant.HasValue && constant.Value is null;
    }

    private static Site? Fail(
        GeneratorSyntaxContext context,
        InvocationExpressionSyntax invocation,
        Receiver receiver,
        ImmutableArray<DiagnosticInfo>.Builder diagnostics,
        System.Threading.CancellationToken token)
    {
        // Diagnostics must survive even when the invocation has no interceptable location
        // (conditional access, for one); an empty InterceptsData marks a diagnostics-only site.
        var location = context.SemanticModel.GetInterceptableLocation(invocation, token);
        return new Site(
            location?.Data ?? string.Empty,
            location?.Version ?? 0,
            receiver,
            Emission: string.Empty,
            diagnostics.ToImmutable());
    }

    private static List<Parameter>? ClassifyParameters(
        IMethodSymbol lambda,
        Dictionary<string, string> routeParameters,
        ArgumentSyntax handlerArgument,
        ImmutableArray<DiagnosticInfo>.Builder diagnostics)
    {
        var result = new List<Parameter>();
        var payloadSeen = false;

        foreach (var parameter in lambda.Parameters)
        {
            // ref/out/in, params, and default values give the lambda a synthesized delegate
            // type that the emitted Action<...>/Func<...> cast cannot invoke — refuse them
            // here instead of throwing InvalidCastException at map time.
            if (parameter.RefKind != RefKind.None || parameter.IsParams || parameter.HasExplicitDefaultValue)
            {
                var shape = parameter.RefKind switch
                {
                    RefKind.Ref => "a 'ref' modifier",
                    RefKind.Out => "an 'out' modifier",
                    RefKind.In => "an 'in' modifier",
                    RefKind.RefReadOnlyParameter => "a 'ref readonly' modifier",
                    _ => parameter.IsParams ? "a 'params' modifier" : "a default value",
                };
                diagnostics.Add(DiagnosticInfo.Create(ParameterShapeUnsupported, handlerArgument.GetLocation(), parameter.Name, shape));
                return null;
            }

            if (!IsEmittable(parameter.Type, out var reason))
            {
                diagnostics.Add(DiagnosticInfo.Create(TypeNotEmittable, handlerArgument.GetLocation(), parameter.Type.ToDisplayString(), reason));
                return null;
            }

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
                    diagnostics.Add(DiagnosticInfo.Create(ParameterNotBindable, handlerArgument.GetLocation(), parameter.Name, "only one parameter can bind the payload"));
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
                    diagnostics.Add(DiagnosticInfo.Create(ParameterNotBindable, handlerArgument.GetLocation(), parameter.Name, $"the template has no route parameter named '{routeName}'"));
                    return null;
                }

                var accessor = RouteAccessorFor(parameter.Type, constraint, routeName);
                if (accessor is null)
                {
                    diagnostics.Add(DiagnosticInfo.Create(
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
                diagnostics.Add(DiagnosticInfo.Create(
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

    /// <summary>
    /// Whether the type can be named from the generated interceptor file: the interceptor is a
    /// non-generic <c>file</c> class in the consumer's assembly, so type parameters and types
    /// less accessible than <c>internal</c> would make the emitted code fail to compile.
    /// </summary>
    private static bool IsEmittable(ITypeSymbol type, out string reason)
    {
        reason = string.Empty;
        switch (type)
        {
            case ITypeParameterSymbol:
                reason = "it is a type parameter, and the generated interceptor is not generic";
                return false;

            case IArrayTypeSymbol array:
                return IsEmittable(array.ElementType, out reason);

            case INamedTypeSymbol named:
                for (var current = named; current is not null; current = current.ContainingType)
                {
                    if (current.IsFileLocal
                        || current.DeclaredAccessibility is Accessibility.Private or Accessibility.Protected or Accessibility.ProtectedAndInternal)
                    {
                        reason = "it is not accessible from generated code (private, protected, or file-local)";
                        return false;
                    }
                }

                foreach (var argument in named.TypeArguments)
                {
                    if (!IsEmittable(argument, out reason))
                    {
                        return false;
                    }
                }

                return true;

            default:
                return true;
        }
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

    /// <summary>
    /// Mirrors <c>MqttRouteTemplate.Parse</c> exactly — every template rejected there must be
    /// rejected here (PMQE005), or the emitted MapMqtt call throws at startup instead of
    /// failing the build.
    /// </summary>
    private static bool TryParseTemplate(string template, out Dictionary<string, string> parameters, out string error)
    {
        parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        error = string.Empty;

        if (template.Length == 0)
        {
            error = "the template is empty";
            return false;
        }

        var segments = template.Split('/');

        if (segments[0] == "$share")
        {
            if (segments.Length < 3)
            {
                error = "a shared subscription needs a group and a topic ('$share/<group>/<topic>')";
                return false;
            }

            var group = segments[1];
            if (group.Length == 0 || group.IndexOfAny(['+', '#', '{', '}']) >= 0)
            {
                error = "the shared subscription group must be a non-empty literal";
                return false;
            }
        }

        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];

            if (segment == "#")
            {
                if (i != segments.Length - 1)
                {
                    error = "'#' is only valid as the last level";
                    return false;
                }

                continue;
            }

            if (segment == "+")
            {
                continue;
            }

            if (segment.Length > 2 && segment[0] == '{' && segment[segment.Length - 1] == '}')
            {
                var body = segment.Substring(1, segment.Length - 2);
                if (body.IndexOf('{') >= 0 || body.IndexOf('}') >= 0)
                {
                    error = $"segment '{segment}' is malformed";
                    return false;
                }

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
                continue;
            }

            if (segment.IndexOf('{') >= 0 || segment.IndexOf('}') >= 0 || segment.IndexOf('+') >= 0 || segment.IndexOf('#') >= 0)
            {
                error = $"segment '{segment}' mixes literals with wildcard or parameter syntax";
                return false;
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

    // The request family: the handler's return value is the reply, so the lowered lambda hands
    // it to MapMqttRequest<TRequest, TResponse> — normalizing Task<T> and plain T returns into
    // the runtime's ValueTask<T> shape.
    private static string EmitRequestInterceptorBody(
        Receiver receiver,
        IMethodSymbol lambda,
        List<Parameter> parameters,
        string returnKind,
        string responseType)
    {
        var payload = parameters.First(p => p.Binding == Binding.Payload);
        var valueTaskType = $"global::System.Threading.Tasks.ValueTask<{responseType}>";
        var returnType = returnKind switch
        {
            "Task" => $"global::System.Threading.Tasks.Task<{responseType}>",
            "ValueTask" => valueTaskType,
            _ => responseType,
        };
        var types = parameters.Select(p => p.Type).Append(returnType);
        var delegateType = $"global::System.Func<{string.Join(", ", types)}>";

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
        var body = returnKind == "ValueTask" ? call : $"new {valueTaskType}({call})";
        var mapCall =
            $"global::Pulse.Mqtt.Endpoints.PulseMqttEndpointExtensions.MapMqttRequest<{payload.Type}, {responseType}>" +
            $"(client, template, (payload, context) => {body}, options, services)";

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
            context.ReportDiagnostic(diagnostic.ToDiagnostic());
        }

        var valid = sites.Where(site => site.Emission.Length > 0 && site.InterceptsData.Length > 0).ToList();
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
