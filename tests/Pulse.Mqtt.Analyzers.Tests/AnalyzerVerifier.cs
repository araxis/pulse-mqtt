using System.Collections.Immutable;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Pulse.Mqtt.Analyzers;
using Shouldly;

namespace Pulse.Mqtt.Analyzers.Tests;

internal static class AnalyzerVerifier
{
    public static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source, CSharpParseOptions.Default);
        var compilation = CSharpCompilation.Create(
            "AnalyzerTests",
            [syntaxTree],
            GetReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        compilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ShouldBeEmpty();

        var analyzers = ImmutableArray.Create<DiagnosticAnalyzer>(new MqttUsageAnalyzer());
        var diagnostics = await compilation
            .WithAnalyzers(analyzers)
            .GetAnalyzerDiagnosticsAsync()
            .ConfigureAwait(false);

        return [.. diagnostics.OrderBy(diagnostic => diagnostic.Location.SourceSpan.Start).ThenBy(diagnostic => diagnostic.Id)];
    }

    private static IEnumerable<MetadataReference> GetReferences()
    {
        var assemblies = ((string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES"))?
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);

        if (assemblies is not null)
        {
            foreach (var assembly in assemblies)
            {
                yield return MetadataReference.CreateFromFile(assembly);
            }

            yield break;
        }

        yield return MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location);
        yield return MetadataReference.CreateFromFile(typeof(Task).GetTypeInfo().Assembly.Location);
        yield return MetadataReference.CreateFromFile(typeof(CancellationToken).GetTypeInfo().Assembly.Location);
    }
}
