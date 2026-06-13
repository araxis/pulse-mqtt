# Versioning and breaking-change policy

Pulse.Mqtt follows [Semantic Versioning 2.0.0](https://semver.org/). From **1.0.0** onward the
public API is a contract.

## What counts as the public API

Every public type and member of the shipped packages, as recorded in each project's
`PublicAPI.Shipped.txt`. The
[`Microsoft.CodeAnalysis.PublicApiAnalyzers`](https://github.com/dotnet/roslyn-analyzers/blob/main/src/PublicApiAnalyzers/PublicApiAnalyzers.Help.md)
analyzer enforces this: an unintended change to the public surface fails the build. New API is added
to `PublicAPI.Unshipped.txt` during development and promoted to `PublicAPI.Shipped.txt` on release.

## The commitment

- **Patch** (`x.y.Z`) — bug fixes only. No API changes.
- **Minor** (`x.Y.0`) — additive only. New types and members; existing ones keep their signatures
  and behavior. Safe to upgrade.
- **Major** (`X.0.0`) — may remove or change existing API. Reserved for genuine breaks, called out
  in the changelog with migration guidance.

A "breaking change" includes removing or renaming a public type/member, changing a signature
(parameters, return type, nullability), tightening behavior a caller could depend on, or removing a
target framework. Adding a member to an interface that callers implement is treated as breaking and
deferred to a major release (or shipped as a default interface method when that is sound).

## Pre-1.0 releases

Before 1.0.0 (the `0.x` line) minor versions could carry breaking changes; the changelog noted them.
That phase is over at 1.0.0.

## Deprecation

Where possible, an API slated for removal is first marked `[Obsolete]` with a pointer to the
replacement for at least one minor release before it is removed in the next major.
