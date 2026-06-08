namespace Pulse.Mqtt;

/// <summary>A recoverable application error identified by a stable, machine-readable code.</summary>
/// <param name="Code">Dot-delimited error code, e.g. <c>connect.not-authorized</c>.</param>
/// <param name="Message">Human-readable description of the failure.</param>
public sealed record AppError(string Code, string Message);

/// <summary>
/// The outcome of an operation that either yields a value or fails with an <see cref="AppError"/>.
/// Used where failure is an expected domain result rather than an exceptional condition.
/// </summary>
/// <typeparam name="T">The success value type.</typeparam>
public readonly record struct Result<T>
{
    private Result(T? value, AppError? error)
    {
        Value = value;
        Error = error;
    }

    /// <summary>The produced value when <see cref="IsSuccess"/> is <see langword="true"/>.</summary>
    public T? Value { get; }

    /// <summary>The failure when <see cref="IsSuccess"/> is <see langword="false"/>.</summary>
    public AppError? Error { get; }

    /// <summary>Whether the operation succeeded.</summary>
    public bool IsSuccess => Error is null;

    /// <summary>Creates a successful result carrying <paramref name="value"/>.</summary>
    public static Result<T> Ok(T value) => new(value, null);

    /// <summary>Creates a failed result with the given code and message.</summary>
    public static Result<T> Fail(string code, string message) => new(default, new AppError(code, message));

    /// <summary>Creates a failed result from an existing <see cref="AppError"/>.</summary>
    public static Result<T> Fail(AppError error) => new(default, error);
}
