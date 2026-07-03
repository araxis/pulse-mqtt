using Pulse.Mqtt.Routing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests;

public sealed class MqttRouteConstraintTests
{
    [Theory]
    [InlineData("sensors/{id:int}/temp", "sensors/42/temp", true)]
    [InlineData("sensors/{id:int}/temp", "sensors/-7/temp", true)]
    [InlineData("sensors/{id:int}/temp", "sensors/oven/temp", false)]
    [InlineData("sensors/{id:int}/temp", "sensors/4.2/temp", false)]
    [InlineData("sensors/{id:long}/temp", "sensors/9223372036854775807/temp", true)]
    [InlineData("sensors/{id:long}/temp", "sensors/9223372036854775808/temp", false)]
    [InlineData("jobs/{id:guid}", "jobs/8f14e45f-ceea-467f-9b3c-91f4f2a1a1aa", true)]
    [InlineData("jobs/{id:guid}", "jobs/not-a-guid", false)]
    [InlineData("flags/{on:bool}", "flags/true", true)]
    [InlineData("flags/{on:bool}", "flags/False", true)]
    [InlineData("flags/{on:bool}", "flags/1", false)]
    public void A_constrained_parameter_only_matches_conforming_levels(string template, string topic, bool matches)
    {
        var route = MqttRouteTemplate.Parse(template);
        route.TryMatch(topic, out _).ShouldBe(matches);
    }

    [Fact]
    public void Captured_values_and_constraints_stay_aligned()
    {
        var route = MqttRouteTemplate.Parse("plants/{plant}/lines/{line:int}/status");

        route.ParameterNames.ShouldBe(["plant", "line"]);
        route.ParameterConstraints.ShouldBe([MqttRouteConstraint.None, MqttRouteConstraint.Int]);

        route.TryMatch("plants/west/lines/3/status", out var values).ShouldBeTrue();
        values["plant"].ShouldBe("west");
        values["line"].ShouldBe("3");
    }

    [Fact]
    public void The_topic_filter_treats_constrained_parameters_as_single_level_wildcards()
    {
        MqttRouteTemplate.Parse("sensors/{id:int}/temp").TopicFilter.ShouldBe("sensors/+/temp");
    }

    [Fact]
    public void Unconstrained_templates_behave_exactly_as_before()
    {
        var route = MqttRouteTemplate.Parse("sensors/{id}/temp");
        route.ParameterConstraints.ShouldBe([MqttRouteConstraint.None]);
        route.TryMatch("sensors/anything-at-all/temp", out var values).ShouldBeTrue();
        values["id"].ShouldBe("anything-at-all");
    }

    [Theory]
    [InlineData("sensors/{id:decimal}/temp")]
    [InlineData("sensors/{id:}/temp")]
    [InlineData("sensors/{:int}/temp")]
    public void Malformed_or_unknown_constraints_fail_parsing(string template)
    {
        Should.Throw<ArgumentException>(() => MqttRouteTemplate.Parse(template));
    }
}
