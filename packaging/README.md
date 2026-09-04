# Packaging inputs for the native distribution

Everything needed to ship `hgl` and the installed SDK through the channels in
[RFC 0032](../docs/source/rfc/rfc_0032_native_distribution.rst) lives here.
**Nothing in this directory is published.** The tap repository does not exist,
no bottle or container image has been pushed, and the user-facing install docs
are not written; pulling that switch is a deliberate, separate step (the
release-switch checklist below). Until then this material is exercised by
`.github/workflows/packaging.yml`, which builds every channel from the checked
out tree and publishes nothing.

## What lives here

| Path | Purpose |
|---|---|
| `homebrew/Formula/hgraph.rb` | The formula: builds the SDK, `hgl` and the analytics extension from a release tarball, stages isocline as a resource so the build runs with FetchContent disconnected, and tests `hgl test` and `hgl --version`. |
| `homebrew/Aliases/hgl` | `brew install hhenson/hgraph/hgl` resolves to the same formula. |
| `homebrew/README.md`, `homebrew/.github/workflows/` | The tap repository's README and its `brew test-bot` / `brew pr-pull` workflows. Copied verbatim into `hhenson/homebrew-hgraph` at the release switch. |
| `docker/Dockerfile` | Multi-stage Debian image with `hgl`, the SDK under `/usr/local` and `g++` so runtime functions compile inside the container. `Dockerfile.dockerignore` trims the build context. |
| `smoke/smoke.hgl` | The one-module smoke every channel runs after installing: `hgl test smoke.hgl` must report `twice_ticks ... ok`. |

The Conan recipe (`conanfile.py` at the repository root, `language=True`
option) is part of the same distribution but stays at the root because Conan
expects it there.

## Contract these inputs implement

- The release version is the bare git tag (`0.8.23`); the formula, the
  container build and the Conan recipe all pass it to CMake as
  `HGRAPH_RELEASE_VERSION`, so `hgl --version` prints
  `hgl <release> (hgraph api <api>)`.
- Every channel configures with `HGRAPH_BUILD_LANGUAGE=ON`,
  `HGRAPH_BUILD_ANALYTICS_EXTENSION=ON`, `HGRAPH_BUILD_SHARED=ON` and Python
  off, and installs the full tree (`bin/hgl`, `include/`, `lib/`,
  `lib/cmake/hgraph`, `lib/cmake/hgl`, `share/hgraph`).
- Homebrew supplies Arrow, fmt, spdlog, simdjson, date and Boost from
  homebrew-core; the container takes Arrow from the Apache apt repository and
  lets the tree fetch the rest because Debian's copies sit below the version
  floors in `CMakeLists.txt`.
- Neither the core libraries nor the language set an absolute rpath. `hgl`
  carries `$ORIGIN/../lib` (`@loader_path/../lib`) so it finds the SDK
  libraries from any prefix; the libraries' own rpath comes from
  `CMAKE_INSTALL_RPATH`, which Homebrew passes, or from installing to a
  default search path, which the container does.

## The dry run

`.github/workflows/packaging.yml` runs on pull requests that touch this
directory, the root or language `CMakeLists.txt`, or `conanfile.py`, and on
`workflow_dispatch`:

- **macOS**: `brew style` and `brew audit --strict` on the formula as written,
  then the formula is copied into a throwaway local tap, pointed at a
  `git archive` of the checkout (a `file://` url with its digest and an
  explicit `version`), installed with `--build-from-source`, and put through
  `brew test`, `brew linkage --test` and the smoke.
- **Linux**: `conan export .`, `docker build` of the image, then
  `hgl --version`, the smoke and `language/examples/midpoint.hgl` inside the
  container.

The formula in the repository points at a tag that does not exist yet with an
all-zero digest; the dry run rewrites those on the runner and never needs the
tag.

## Release-switch checklist (RFC 0032, R1-R5)

Do these in order, once, when the release is wanted. None of them is
automated on purpose.

1. **R1** Tag the release (`git tag 0.8.23 && git push origin 0.8.23`) and
   let the existing release workflow publish the wheels and the GitHub
   release with the source tarball.
2. **R2** Create `hhenson/homebrew-hgraph` from `packaging/homebrew/`
   (formula, alias, README, workflows), set the formula's `url` to the tag's
   tarball and `sha256` to its digest, and open the first pull request in the
   tap. `brew test-bot` builds the bottles; labelling the pull request
   `pr-pull` publishes them to the tap's GitHub release.
3. **R3** Build and push the container image from `packaging/docker/`
   (`docker build --build-arg HGRAPH_RELEASE_VERSION=0.8.23` then push to
   `ghcr.io/hhenson/hgl:0.8.23` and `:latest`).
4. **R4** Add the "Installing hgl" page to the user guide with the
   `brew install hhenson/hgraph/hgl`, `docker run ghcr.io/hhenson/hgl` and
   Conan lines, and the native channels line to `release_readiness.rst`.
5. **R5** Mark RFC 0032 Implemented and record the first published release
   in its implementation status.

After the switch, keep `packaging/homebrew/` and the tap in step: the tap is
the published copy, this directory is where changes are reviewed with the
code they depend on.
