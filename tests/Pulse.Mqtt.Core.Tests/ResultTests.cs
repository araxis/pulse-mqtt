using Pulse.Mqtt;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Core.Tests;

public sealed class ResultTests
{
    [Fact]
    public void Ok_carries_value_and_succeeds()
    {
        var result = Result<int>.Ok(42);

        result.IsSuccess.ShouldBeTrue();
        result.Value.ShouldBe(42);
        result.Error.ShouldBeNull();
    }

    [Fact]
    public void Fail_carries_error_and_does_not_succeed()
    {
        var result = Result<int>.Fail("connect.not-authorized", "rejected");

        result.IsSuccess.ShouldBeFalse();
        result.Error.ShouldNotBeNull();
        result.Error!.Code.ShouldBe("connect.not-authorized");
        result.Error.Message.ShouldBe("rejected");
    }

    [Fact]
    public void Fail_from_error_preserves_the_error()
    {
        var error = new AppError("x.y", "boom");

        var result = Result<string>.Fail(error);

        result.Error.ShouldBe(error);
    }
}
