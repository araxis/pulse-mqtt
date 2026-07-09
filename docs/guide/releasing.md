# Releasing

How this repository ships.

## Pipelines

- **CI** (`.github/workflows/ci.yml`): every push and pull request to `main` builds the
  solution, runs all test projects (the integration tests start Mosquitto in Docker on the
  runner), and uploads the packages as a build artifact.
- **Broker matrix** (`.github/workflows/broker-matrix.yml`): source or integration-test changes
  run the heavier cross-broker tests. A failed `main` run opens or updates a tracking issue, and
  the next successful `main` run closes that issue with the recovery run link.
- **Release** (`.github/workflows/release.yml`) publishes in two modes:
  - **Stable** — pushing a tag that starts with `v` builds, tests, packs every packable
    project with the tag's version, pushes to nuget.org, and creates a GitHub release whose
    notes come from the matching `CHANGELOG.md` section, with the packages attached.
  - **Prerelease** — every push to `main` publishes `X.Y.Z-preview.<run-number>` (the
    `X.Y.Z` from `VersionPrefix` in `Directory.Build.props`) to nuget.org, with no GitHub
    release. Prereleases are excluded from `dotnet add package` by default, so consumers keep
    getting the last stable version unless they opt in with `--prerelease`.

## One-time setup

Publishing uses NuGet [trusted publishing](https://learn.microsoft.com/nuget/nuget-org/trusted-publishing):
the workflow exchanges a short-lived GitHub OIDC token for a one-hour nuget.org API key at
push time, so there is **no long-lived secret to store or rotate**.

1. On **nuget.org**, open your profile → **Trusted Publishing** and add a policy:
   - **Repository Owner:** `araxis`
   - **Repository:** `pulse-mqtt`
   - **Workflow File:** `release.yml` (file name only)
   - **Environment:** leave empty
2. Add one repository secret, `NUGET_USER`, set to your nuget.org account name (the profile
   name, **not** an email). It identifies the account but is not itself a credential.

The workflow already requests the token (`permissions: id-token: write`) and logs in with
`NuGet/login@v1` before pushing.

::: tip First publish on a private repository
A new policy on a private repository is provisional for 7 days until the first successful
publish locks it to the repository and owner IDs. Publish once within that window, or restart
it from the nuget.org UI.
:::

## Cutting a release

1. Update `CHANGELOG.md`: add a `## <version>` section describing the release.
2. Merge to `main` through a pull request as usual.
3. Tag and push:

   ```shell
   git tag v0.1.0
   git push origin v0.1.0
   ```

The workflow does the rest. `--skip-duplicate` makes re-running safe: versions already on
nuget.org are left alone.

Between stable releases, each merge to `main` ships a `-preview.<run>` build automatically, so
there is always a fresh package to test against without burning a stable version number.

## Versioning

**All packages share one version and ship together (lockstep).** A single `VersionPrefix` in
`Directory.Build.props` drives every package, one `vX.Y.Z` tag releases the whole set, and each
add-on's dependency on `Pulse.Mqtt.Core` is stamped to that same version. This is deliberate:
the packages are one tightly-coupled family (everything builds on `Core`), so lockstep keeps
"what version am I on" a single answer, keeps a given version a set that was tested together,
and matches how coupled .NET families such as `Microsoft.Extensions.*` ship. The alternative —
per-package versions — was considered and rejected: it buys little here because most changes
touch `Core` and cascade anyway, and it turns bug reports into an N-dimensional support matrix.

`Directory.Build.props` carries `VersionPrefix` for local builds; the release workflow overrides
the version with the tag (`v1.2.3` → `1.2.3`, `v1.2.3-rc.1` → `1.2.3-rc.1`). Keep `VersionPrefix`
in step with the next planned release after tagging.

### Release only on a shipped change

Because a stable tag republishes every package, **cut a release only when shipped content
actually changed since the last tag** — code under `src/`, package README files under `src/`,
dependency versions in `Directory.Packages.props`, shared package metadata in
`Directory.Build.props` (icon, tags, license, ...), or a packed root asset (`icon.png`). Docs,
tests, samples, and CI changes
do not ship in the packages, so they do not warrant a release on their own; let them ride the
next one that does. Version bumps and `PublicAPI` ledger promotions are release bookkeeping,
not shipped changes — and neither is the `VersionPrefix` line in `Directory.Build.props`, which
changes at every release by construction; the guard below diffs that file's content with that
one line filtered out; a genuine metadata change there still counts.

The release workflow enforces this: a stable tag whose shipped content is identical to the
previous tag fails fast with a clear message, rather than publishing a fleet of byte-identical
packages under a new number. For the rare intentional re-publish (for example, recovering from a
botched push), put `[republish]` in the tag message to bypass the guard.

## This documentation site

The site is a VitePress project living in `docs/`:

```shell
cd docs
npm install
npm run docs:dev       # live-reload authoring at localhost:5173
npm run docs:build     # static site into docs/.vitepress/dist
```

`.github/workflows/docs.yml` builds the site on every pull request for verification and
**deploys to GitHub Pages automatically on every merge to `main`** that touches `docs/**`
(also available on manual dispatch). Pages must be enabled on the repository with "GitHub
Actions" as the source.
