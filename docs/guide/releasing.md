# Releasing

How this repository ships.

## Pipelines

- **CI** (`.github/workflows/ci.yml`): every push and pull request to `main` builds the
  solution, runs all test projects (the integration tests start Mosquitto in Docker on the
  runner), and uploads the packages as a build artifact.
- **Release** (`.github/workflows/release.yml`): pushing a tag that starts with `v` builds,
  tests, packs every packable project with the tag's version, pushes the packages to
  nuget.org, and creates a GitHub release whose notes come from the matching `CHANGELOG.md`
  section, with the packages attached.

## One-time setup

Add a repository secret named `NUGET_API_KEY` holding a nuget.org API key with push rights for
the `Pulse.Mqtt.*` packages (Settings → Secrets and variables → Actions).

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

## Versioning

`Directory.Build.props` carries `VersionPrefix` for local builds; the release workflow
overrides the version with the tag (`v1.2.3` → `1.2.3`, `v1.2.3-rc.1` → `1.2.3-rc.1`). Keep
`VersionPrefix` in step with the next planned release after tagging.

## This documentation site

The site is a VitePress project living in `docs/`:

```shell
cd docs
npm install
npm run docs:dev       # live-reload authoring at localhost:5173
npm run docs:build     # static site into docs/.vitepress/dist
```

`.github/workflows/docs.yml` builds the site on every change for verification and deploys to
GitHub Pages on manual dispatch (Pages must be enabled on the repository, with "GitHub
Actions" as the source).
