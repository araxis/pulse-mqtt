using Pulse.Mqtt.Routing;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests.Routing;

public sealed class MqttRouteTemplateTests
{
    [Fact]
    public void Parses_parameters_and_builds_the_subscription_filter()
    {
        var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp/{unit}");

        template.TopicFilter.ShouldBe("sensors/+/temp/+");
        template.ParameterNames.ShouldBe(["deviceId", "unit"]);
    }

    [Fact]
    public void Captures_parameter_values()
    {
        var template = MqttRouteTemplate.Parse("sensors/{deviceId}/temp");

        template.TryMatch("sensors/device-42/temp", out var values).ShouldBeTrue();

        values.Count.ShouldBe(1);
        values["deviceId"].ShouldBe("device-42");
    }

    [Fact]
    public void Captures_multiple_parameters_in_order()
    {
        var template = MqttRouteTemplate.Parse("{tenant}/devices/{deviceId}/state");

        template.TryMatch("acme/devices/d1/state", out var values).ShouldBeTrue();

        values["tenant"].ShouldBe("acme");
        values["deviceId"].ShouldBe("d1");
        values.TryGetValue("missing", out _).ShouldBeFalse();
    }

    [Fact]
    public void Rejects_topics_that_do_not_match()
    {
        var template = MqttRouteTemplate.Parse("sensors/{id}/temp");

        template.TryMatch("sensors/a/humidity", out _).ShouldBeFalse();
        template.TryMatch("sensors/a", out _).ShouldBeFalse();
        template.TryMatch("sensors/a/temp/extra", out _).ShouldBeFalse();
    }

    [Fact]
    public void Plain_wildcards_still_work()
    {
        var template = MqttRouteTemplate.Parse("a/+/c/#");

        template.TopicFilter.ShouldBe("a/+/c/#");
        template.TryMatch("a/b/c/d/e", out _).ShouldBeTrue();
        template.TryMatch("a/b/c", out _).ShouldBeTrue();   // trailing '#' matches the parent
        template.TryMatch("a/b/x", out _).ShouldBeFalse();
    }

    [Fact]
    public void Parameter_templates_do_not_match_reserved_topics()
    {
        var template = MqttRouteTemplate.Parse("{root}/broker");

        template.TryMatch("$SYS/broker", out _).ShouldBeFalse();
    }

    [Fact]
    public void Literal_only_templates_match_exactly()
    {
        var template = MqttRouteTemplate.Parse("plant/line1/status");

        template.TopicFilter.ShouldBe("plant/line1/status");
        template.TryMatch("plant/line1/status", out var values).ShouldBeTrue();
        values.Count.ShouldBe(0);
    }

    [Theory]
    [InlineData("a/{}/b")]          // empty parameter name
    [InlineData("a/{x/b")]          // unclosed brace
    [InlineData("a/x}/b")]          // stray brace
    [InlineData("a/{x}{y}/b")]      // not a whole-segment parameter
    [InlineData("a/#/b")]           // '#' not last
    [InlineData("a/b+/c")]          // '+' inside a literal
    [InlineData("a/{x}/{x}")]       // duplicate name
    public void Rejects_malformed_templates(string template)
    {
        Should.Throw<ArgumentException>(() => MqttRouteTemplate.Parse(template));
    }

    [Fact]
    public void Shared_subscription_templates_match_the_delivered_topic()
    {
        // The broker delivers to a '$share/<group>/<filter>' subscriber with the topic alone —
        // the '$share/<group>/' prefix belongs in the SUBSCRIBE filter only.
        var template = MqttRouteTemplate.Parse("$share/workers/jobs/{id:int}");

        template.TopicFilter.ShouldBe("$share/workers/jobs/+");
        template.TryMatch("jobs/42", out var values).ShouldBeTrue();
        values["id"].ShouldBe("42");

        template.TryMatch("$share/workers/jobs/42", out _).ShouldBeFalse();
        template.TryMatch("jobs/not-a-number", out _).ShouldBeFalse();
    }

    [Fact]
    public void Shared_subscription_templates_support_wildcards()
    {
        var template = MqttRouteTemplate.Parse("$share/g/alerts/#");

        template.TopicFilter.ShouldBe("$share/g/alerts/#");
        template.TryMatch("alerts/plant/line1", out _).ShouldBeTrue();
        template.TryMatch("$SYS/alerts/x", out _).ShouldBeFalse(); // still no $-topics via wildcards
    }

    [Theory]
    [InlineData("$share")]           // no group, no topic
    [InlineData("$share/g")]         // no topic
    [InlineData("$share//x")]        // empty group
    [InlineData("$share/{g}/x")]     // parameter as group
    [InlineData("$share/g+h/x")]     // wildcard in group
    public void Rejects_malformed_shared_subscription_templates(string template)
    {
        Should.Throw<ArgumentException>(() => MqttRouteTemplate.Parse(template));
    }
}
