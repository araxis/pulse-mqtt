using System.Collections.Immutable;
using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Pulse.Mqtt.Analyzers;

/// <summary>Reports conservative usage warnings for Pulse MQTT applications.</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class MqttUsageAnalyzer : DiagnosticAnalyzer
{
    private const string Category = "Pulse.Mqtt.Usage";
    private const string HelpLinkBase = "https://araxis.github.io/pulse-mqtt/guide/analyzers.html";

    private static readonly DiagnosticDescriptor UnobservedAsyncOperation = new(
        "PMQ0001",
        "Observe MQTT async operations",
        "Observe the returned task from '{0}'",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Pulse MQTT async operations should be awaited, returned, assigned, passed to another API, or explicitly discarded.",
        helpLinkUri: HelpLinkBase + "#pmq0001-observe-mqtt-async-operations");

    private static readonly DiagnosticDescriptor MissingCancellationToken = new(
        "PMQ0002",
        "Pass caller cancellation token",
        "Pass the available CancellationToken to '{0}'",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Pulse MQTT async operations with an optional CancellationToken should receive the caller token when one is available.",
        helpLinkUri: HelpLinkBase + "#pmq0002-pass-caller-cancellation-token");

    private static readonly DiagnosticDescriptor SynchronousDispose = new(
        "PMQ0003",
        "Use async disposal for MQTT async-owned resources",
        "Dispose '{0}' asynchronously",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Pulse MQTT async-owned resources should be disposed with DisposeAsync or await using.",
        helpLinkUri: HelpLinkBase + "#pmq0003-use-async-disposal");

    private static readonly DiagnosticDescriptor Mqtt5PropertyOnMqtt311Packet = new(
        "PMQ0004",
        "Do not use MQTT 5-only properties on MQTT 3.1.1 packets",
        "MQTT 5-only property '{0}' is set on '{1}' while ProtocolVersion is V311",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "MQTT 5-only packet properties are not encoded on MQTT 3.1.1 packets. Use MQTT 5.0 or remove the property.",
        helpLinkUri: HelpLinkBase + "#pmq0004-do-not-use-mqtt-5-only-properties-on-mqtt-3-1-1-packets");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(
            UnobservedAsyncOperation,
            MissingCancellationToken,
            SynchronousDispose,
            Mqtt5PropertyOnMqtt311Packet);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeUsingStatement, SyntaxKind.UsingStatement);
        context.RegisterSyntaxNodeAction(AnalyzeUsingDeclaration, SyntaxKind.LocalDeclarationStatement);
        context.RegisterSyntaxNodeAction(
            AnalyzeObjectCreation,
            SyntaxKind.ObjectCreationExpression,
            SyntaxKind.ImplicitObjectCreationExpression);
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        var invocationSyntax = (InvocationExpressionSyntax)context.Node;
        if (context.SemanticModel.GetOperation(invocationSyntax, context.CancellationToken)
            is not IInvocationOperation invocation)
        {
            return;
        }

        var method = invocation.TargetMethod;

        if (PulseMqttApiFacts.IsPulseMqttAsyncOperation(method))
        {
            if (IsBareExpressionStatement(invocation.Syntax))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    UnobservedAsyncOperation,
                    invocation.Syntax.GetLocation(),
                    method.Name));
            }

            if (PulseMqttApiFacts.HasOptionalFinalCancellationToken(method)
                && !SuppliesCancellationToken(invocation)
                && HasAvailableCancellationToken(invocation, context))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    MissingCancellationToken,
                    invocation.Syntax.GetLocation(),
                    method.Name));
            }
        }

        if (method.Name == nameof(IDisposable.Dispose)
            && method.Parameters.Length == 0
            && invocation.Instance?.Type is { } disposedType
            && PulseMqttApiFacts.IsKnownAsyncOwnedResource(disposedType))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                SynchronousDispose,
                invocation.Syntax.GetLocation(),
                disposedType.Name));
        }
    }

    private static void AnalyzeUsingStatement(SyntaxNodeAnalysisContext context)
    {
        var usingStatement = (UsingStatementSyntax)context.Node;
        if (usingStatement.AwaitKeyword.RawKind != 0)
        {
            return;
        }

        if (TryGetUsingStatementResourceType(usingStatement, context, out var resourceType)
            && PulseMqttApiFacts.IsKnownAsyncOwnedResource(resourceType))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                SynchronousDispose,
                usingStatement.GetLocation(),
                resourceType.Name));
        }
    }

    private static void AnalyzeUsingDeclaration(SyntaxNodeAnalysisContext context)
    {
        var localDeclaration = (LocalDeclarationStatementSyntax)context.Node;
        if (localDeclaration.UsingKeyword.RawKind == 0
            || localDeclaration.AwaitKeyword.RawKind != 0)
        {
            return;
        }

        foreach (var variable in localDeclaration.Declaration.Variables)
        {
            if (context.SemanticModel.GetDeclaredSymbol(variable, context.CancellationToken)
                    is ILocalSymbol { Type: { } resourceType }
                && PulseMqttApiFacts.IsKnownAsyncOwnedResource(resourceType))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    SynchronousDispose,
                    localDeclaration.GetLocation(),
                    resourceType.Name));
                return;
            }
        }
    }

    private static void AnalyzeObjectCreation(SyntaxNodeAnalysisContext context)
    {
        var objectCreation = (BaseObjectCreationExpressionSyntax)context.Node;
        if (objectCreation.Initializer is not { } initializer)
        {
            return;
        }

        var type = context.SemanticModel.GetTypeInfo(objectCreation, context.CancellationToken).Type;
        if (type is null
            || !PulseMqttApiFacts.TryGetMqtt5OnlyPacketProperties(type, out var mqtt5OnlyProperties)
            || !InitializerSetsProtocolVersionV311(initializer, context))
        {
            return;
        }

        foreach (var expression in initializer.Expressions)
        {
            if (TryGetAssignedPropertyName(expression, out var propertyName)
                && IsMqtt5OnlyProperty(mqtt5OnlyProperties, propertyName))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Mqtt5PropertyOnMqtt311Packet,
                    expression.GetLocation(),
                    propertyName,
                    type.Name));
            }
        }
    }

    private static bool IsBareExpressionStatement(SyntaxNode syntax) =>
        syntax.Parent is ExpressionStatementSyntax;

    private static bool SuppliesCancellationToken(IInvocationOperation invocation)
    {
        foreach (var argument in invocation.Arguments)
        {
            if (argument.ArgumentKind == ArgumentKind.DefaultValue)
            {
                continue;
            }

            if (argument.Parameter?.Type is { } parameterType
                && PulseMqttApiFacts.IsCancellationToken(parameterType))
            {
                return true;
            }
        }

        return false;
    }

    private static bool HasAvailableCancellationToken(
        IInvocationOperation invocation,
        SyntaxNodeAnalysisContext context)
    {
        var symbols = context.SemanticModel.LookupSymbols(invocation.Syntax.SpanStart);

        foreach (var symbol in symbols)
        {
            if (symbol is IParameterSymbol parameter
                && PulseMqttApiFacts.IsCancellationToken(parameter.Type))
            {
                return true;
            }

            if (symbol is ILocalSymbol local
                && PulseMqttApiFacts.IsCancellationToken(local.Type)
                && IsCancellationTokenName(local.Name))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsCancellationTokenName(string name) =>
        name is "cancellationToken" or "ct" or "token";

    private static bool InitializerSetsProtocolVersionV311(
        InitializerExpressionSyntax initializer,
        SyntaxNodeAnalysisContext context)
    {
        foreach (var expression in initializer.Expressions)
        {
            if (TryGetAssignedPropertyName(expression, out var propertyName)
                && propertyName == "ProtocolVersion"
                && expression is AssignmentExpressionSyntax assignment
                && IsMqtt311ProtocolVersion(assignment.Right, context))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsMqtt311ProtocolVersion(
        ExpressionSyntax expression,
        SyntaxNodeAnalysisContext context)
    {
        var constant = context.SemanticModel.GetConstantValue(expression, context.CancellationToken);
        if (!constant.HasValue || constant.Value is null)
        {
            return false;
        }

        try
        {
            return Convert.ToInt64(constant.Value, CultureInfo.InvariantCulture) == 4;
        }
        catch (FormatException)
        {
            return false;
        }
        catch (InvalidCastException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    private static bool TryGetAssignedPropertyName(ExpressionSyntax expression, out string propertyName)
    {
        if (expression is AssignmentExpressionSyntax assignment)
        {
            if (assignment.Left is IdentifierNameSyntax identifier)
            {
                propertyName = identifier.Identifier.ValueText;
                return true;
            }

            if (assignment.Left is MemberAccessExpressionSyntax { Name: IdentifierNameSyntax member })
            {
                propertyName = member.Identifier.ValueText;
                return true;
            }
        }

        propertyName = string.Empty;
        return false;
    }

    private static bool IsMqtt5OnlyProperty(string[] propertyNames, string propertyName)
    {
        foreach (var candidate in propertyNames)
        {
            if (candidate == propertyName)
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryGetUsingStatementResourceType(
        UsingStatementSyntax usingStatement,
        SyntaxNodeAnalysisContext context,
        out ITypeSymbol resourceType)
    {
        if (usingStatement.Expression is { } expression)
        {
            resourceType = context.SemanticModel.GetTypeInfo(expression, context.CancellationToken).Type!;
            return resourceType is not null;
        }

        if (usingStatement.Declaration is { } declaration)
        {
            foreach (var variable in declaration.Variables)
            {
                if (context.SemanticModel.GetDeclaredSymbol(variable, context.CancellationToken)
                    is ILocalSymbol { Type: { } declaredType })
                {
                    resourceType = declaredType;
                    return true;
                }
            }
        }

        resourceType = null!;
        return false;
    }
}
