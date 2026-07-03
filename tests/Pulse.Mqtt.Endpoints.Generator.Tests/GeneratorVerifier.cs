using System.Collections.Immutable;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Pulse.Mqtt.Endpoints.Generator;
using Shouldly;

namespace Pulse.Mqtt.Endpoints.Generator.Tests;

internal static class GeneratorVerifier
{
    /// <summary>
    /// Runs the MapMqtt generator over the source and returns its diagnostics plus whether any
    /// interceptor source was emitted. The input must itself compile — a broken test fixture
    /// fails loudly instead of shadowing the diagnostic under test.
    /// </summary>
    public static (ImmutableArray<Diagnostic> Diagnostics, bool Emitted) Run(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source, CSharpParseOptions.Default);
        var compilation = CSharpCompilation.Create(
            "GeneratorTests",
            [syntaxTree],
            GetReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        compilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ShouldBeEmpty();

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MapMqttGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out var diagnostics);

        var emitted = driver.GetRunResult().GeneratedTrees.Any(tree => tree.FilePath.Contains("MapMqttInterceptors"));
        return ([.. diagnostics.OrderBy(d => d.Id)], emitted);
    }

    /// <summary>
    /// Runs the generator over two consecutive compilations sharing one driver, so incremental
    /// caching between runs behaves as it does in the IDE: the caller tree is unchanged, the
    /// other tree is swapped. Returns each run's diagnostics.
    /// </summary>
    public static (ImmutableArray<Diagnostic> First, ImmutableArray<Diagnostic> Second) RunTwice(
        string callerSource,
        string firstOtherSource,
        string secondOtherSource)
    {
        var caller = CSharpSyntaxTree.ParseText(callerSource, CSharpParseOptions.Default, path: "Caller.cs");
        var firstOther = CSharpSyntaxTree.ParseText(firstOtherSource, CSharpParseOptions.Default, path: "Other.cs");
        var compilation = CSharpCompilation.Create(
            "GeneratorTests",
            [caller, firstOther],
            GetReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        compilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ShouldBeEmpty();

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MapMqttGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out var first);

        var secondOther = CSharpSyntaxTree.ParseText(secondOtherSource, CSharpParseOptions.Default, path: "Other.cs");
        var updated = compilation.ReplaceSyntaxTree(firstOther, secondOther);

        updated.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ShouldBeEmpty();

        driver.RunGeneratorsAndUpdateCompilation(updated, out _, out var second);
        return (first, second);
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
    }
}
