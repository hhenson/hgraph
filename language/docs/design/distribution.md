# Distribution and deployment

Status: proposed (2026-09-04). Nothing in this record is implemented unless
a section says so; the work items at the end are the implementation order.

This record covers how the toolchain reaches users, how its versions are
named, and what a host needs to run an HGL program. It does not cover module
descriptors or HGL package dependencies ([Modules and native
extensions](modules.md)), nor the Python wheel pipeline itself
(`docs/source/developer_guide/release_readiness.rst` in the hgraph tree).

## What exists today

The repository already ships three things the language can build on:

- **The wheel pipeline.** `release-wheels.yml` builds `hgraph` and its four
  extension distributions as cp312-abi3 wheels for Linux (manylinux_2_28),
  macOS arm64, and Windows on every bare-version tag (`0.8.22` is the
  latest) and restamps them to that version. The source distribution
  excludes `language/` (Slice 0 acceptance), so the wheel channel currently
  knows nothing about `hgl`.
- **The Conan recipe.** `conanfile.py` packages the shared-library SDK
  (libraries, headers, debugger support, `hgraphConfig.cmake`) with all
  dependencies from Conan Center and Python off. It does not export
  `language/`, and its version comes from `v_*` tags, which only the 0.5
  line carries; from C++-first `main` it yields `0.8.0.dev<n>`.
- **The install tree.** `cmake --install` of a Python-free build with
  `HGRAPH_BUILD_LANGUAGE=ON` produces one prefix:

  ```text
  bin/hgl                          component Runtime
  include/hgraph, include/third_party, include/date
  lib/libhgraph_{runtime,wiring,stdlib}.*   (+ analytics when enabled)
  lib/cmake/hgraph/hgraphConfig.cmake       find_dependency(fmt 11, Arrow,
                                            ArrowCompute, ArrowAcero,
                                            spdlog 1.15, simdjson, date)
  lib/cmake/hgl/HglLanguage.cmake           component Development
  share/hgraph/debugger
  ```

  `language.yml` already installs `--component Runtime` and smokes
  `hgl --version` on Linux and macOS.

Two facts about `hgl` itself shape everything below:

1. **`hgl` has shared runtime dependencies.** Apache Arrow (with compute and
   acero) is a formal dependency of the core, and the tree deliberately
   does not fetch it. A Python-free `hgl` links the Arrow shared libraries
   plus fmt, spdlog, simdjson, and date/tz from the system. There is no
   single-file static binary without a static Arrow build, and this record
   does not propose one.
2. **Scripted runtime functions will compile C++ at run time.** The
   scripted loader is not on `main` yet: #640 and #641 (open) lower
   runtime functions to C++, compile them with the C++ compiler recorded
   when `hgl` was built, and load the image. Once they land, a deployed
   `hgl` needs a C++23 compiler, the SDK headers, and every dependency's
   headers on the host, not only its libraries; the channels below are
   shaped for that from the start rather than retrofitted.

## Principles

1. **One release train.** `hgl` versions with hgraph and is built from the
   same tag. It links the core it was built with, the SDK is
   source-provisional with an exact-version pin (RFC 0005), and generated
   packages carry the version they were built against; a separately
   versioned `hgl` would need a compatibility matrix nobody can promise
   yet. The language *edition*, the
   syntax-compatibility notion the roadmap's release gates ask for, is a
   separate name recorded in the module header when it exists; it is not
   the tool's version.
2. **Package managers own dependencies.** Because Arrow is shared and heavy,
   a downloadable tarball of `hgl` would have to bundle Arrow, compute,
   acero, and their transitive libraries. Every channel below is a package
   manager (or a container image) that resolves those dependencies itself.
   GitHub Releases carry the tagged source archive and checksums, nothing
   prebuilt.
3. **One package carries `hgl` and the SDK.** `hgl` links the core
   libraries, the scripted path needs the SDK headers, and AOT packages
   need `lib/cmake`. Splitting them into two packages would double the
   core build and invite version skew between the two halves.
4. **Native channels have no Python.** Python users get the wheel. Whether
   the wheel should also ship an `hgl` (the Python-embedded flavour) is an
   open decision recorded below, not a plan.
5. **Relocatable by construction.** Anything `hgl` records at build time
   about compilers and paths must survive being installed on a machine that
   did not build it. Bottles, Conan packages, and container images are all
   built somewhere else.

## Artifacts

| Artifact | Contents | Install component | Channels |
|---|---|---|---|
| `hgl` toolchain | `bin/hgl` | `Runtime` | Homebrew, Conan (option), container image |
| SDK | headers, libraries, `lib/cmake/hgraph`, `lib/cmake/hgl` | `Development` | same package as `hgl` |
| Python distribution | `hgraph` wheel and extension wheels | — | PyPI, unchanged |

Both native artifacts come from one `cmake --install` of one configure; the
formula and the recipe below are two spellings of that configure.

## Versioning

- The release version is the bare git tag (`0.8.23`), the same authority
  `release-wheels.yml` and `tools/validate_release.py` already use.
- The core build gains `HGRAPH_RELEASE_VERSION`, a cache variable defaulting
  to `git describe --tags --match '[0-9]*'` when the source is a git
  checkout (a commit past the tag reports `0.8.22-61-g7b9ebd6`), and to
  `PROJECT_VERSION` with a `-dev` suffix otherwise. Packagers pass it
  explicitly. `include/hgraph/version.h.in` exposes it beside the native
  API version, which stays `0.8.0` and keeps its `SameMajorVersion` role in
  `hgraphConfigVersion.cmake`.
- `hgl --version` prints the release version first and the API version as
  the qualifier: `hgl 0.8.23 (hgraph api 0.8.0)`. The language project's own
  `VERSION 0.1.0` goes away; it has no independent meaning once the tool
  rides the hgraph train.
- The Conan recipe's `set_version` moves to the bare tags for the same
  reason, so `conan create` on a tagged commit yields the release version.

## Channels

### Homebrew

The first channel, because it is the one a macOS developer reaches for
and the one whose sandbox forces the relocatability work anyway.

**Shape.** A tap, `hhenson/homebrew-hgraph`, with one formula `hgraph`
providing `bin/hgl` and the SDK, and a tap alias `hgl` so both
`brew install hhenson/hgraph/hgraph` and `brew install hhenson/hgraph/hgl`
work. Bottles are built by the tap's own GitHub Actions (`brew test-bot`,
the workflow `brew tap-new` generates) for arm64 macOS and x86_64/arm64
Linux, so installs do not compile the core locally. homebrew-core is a
1.0 question: it wants a stable release history and no experimental
flags, neither of which the language has yet.

**Formula sketch.** Every dependency exists in homebrew-core already:
`apache-arrow` (25.0.1, built with compute and acero), `fmt`, `spdlog`,
`simdjson`, `howard-hinnant-date` (built as the tz library over the
system database, matching the Conan configuration), and `boost` at build
time only: the analytics kernels use header-only Boost.Math (floor 1.90),
which the tree otherwise fetches, and `FETCHCONTENT_FULLY_DISCONNECTED`
would fail the configure without it. Isocline is not, and Homebrew's
build sandbox has no network, so the REPL's line editor is a formula
resource handed to FetchContent:

```ruby
class Hgraph < Formula
  desc "hgraph runtime SDK and the hgl language toolchain"
  homepage "https://github.com/hhenson/hgraph"
  url "https://github.com/hhenson/hgraph/archive/refs/tags/0.8.23.tar.gz"
  sha256 "<archive digest>"
  license "MIT"
  head "https://github.com/hhenson/hgraph.git", branch: "main"

  depends_on "boost" => :build # Boost.Math, header-only, analytics kernels
  depends_on "cmake" => :build
  depends_on "ninja" => :build
  depends_on "apache-arrow"
  depends_on "fmt"
  depends_on "howard-hinnant-date"
  depends_on "simdjson"
  depends_on "spdlog"
  on_linux do
    depends_on "gcc"   # scripted runtime functions compile at run time
  end

  resource "isocline" do
    url "https://github.com/daanx/isocline/archive/refs/tags/v1.1.0.tar.gz"
    sha256 "<archive digest>"
  end

  def install
    (buildpath/"isocline").install resource("isocline")
    args = %W[
      -DHGRAPH_RELEASE_VERSION=#{version}
      -DHGRAPH_BUILD_LANGUAGE=ON
      -DHGRAPH_BUILD_ANALYTICS_EXTENSION=ON
      -DHGRAPH_BUILD_SHARED=ON
      -DHGRAPH_BUILD_PYTHON_BINDINGS=OFF
      -DHGRAPH_ENABLE_PYTHON_USER_NODES=OFF
      -DHGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS=OFF
      -DHGRAPH_USE_PYARROW_ARROW=OFF
      -DHGRAPH_FETCH_SIMDJSON=OFF
      -DHGRAPH_FETCH_DATE=OFF
      -DHGRAPH_TIME_ZONE_BACKEND=date
      -DHGRAPH_ENABLE_COMPILER_CACHE=OFF
      -DBUILD_TESTING=OFF
      -DFETCHCONTENT_FULLY_DISCONNECTED=ON
      -DFETCHCONTENT_SOURCE_DIR_ISOCLINE=#{buildpath}/isocline
    ]
    system "cmake", "-S", ".", "-B", "build", "-G", "Ninja",
           *args, *std_cmake_args
    system "cmake", "--build", "build"
    system "cmake", "--install", "build"
  end

  test do
    (testpath/"smoke.hgl").write <<~HGL
      module smoke

      fn twice(x: f64) -> f64 => x * 2.0

      test twice_ticks {
          assert eval(twice, [1.0, 2.0]) == [2.0, 4.0]
      }
    HGL
    assert_match "twice_ticks ... ok", shell_output("#{bin}/hgl test smoke.hgl")
    assert_match version.to_s, shell_output("#{bin}/hgl --version")
  end
end
```

Notes on the sketch:

- `HGRAPH_BUILD_SHARED=ON` so the SDK libraries in the prefix are the ones
  `hgl` and AOT packages share; Homebrew's `std_cmake_args` sets the
  install rpath.
- Analytics is on because the examples import `hgraph.analytics` and the
  language CI builds it; the other first-party extensions bring
  dependencies (librdkafka, object stores) that stay out of the formula
  until a module descriptor asks for them.
- `HGRAPH_TIME_ZONE_BACKEND=date` mirrors the Conan recipe: one named-zone
  backend across platforms instead of a per-machine probe.
- The formula must be verified against the current homebrew-core versions
  before it is published: `find_package(fmt 11)` accepts fmt 12 through
  fmt's `AnyNewerVersion` config, but the tree is only built against fmt
  11 today, and the compiler is Apple Clang on macOS and the `gcc` formula
  on Linux, neither of which is the CI toolchain.
- `test do` is the formula's acceptance test and the smallest program the
  first pass runs; it should keep working across editions or the formula
  pins an edition.

**Runtime toolchain.** Homebrew presumes the Xcode command line tools on
macOS and installs `gcc` on Linux, so the scripted compiler is available
wherever the formula is. The formula neither bundles nor pins a compiler;
the relocatable native context below resolves it on the user's machine.

### Conan

The recipe already packages the SDK for C++ consumers. It gains a boolean
option `language` (default off) that exports `language/*`, configures with
`HGRAPH_BUILD_LANGUAGE=ON`, and installs `bin/hgl` and `lib/cmake/hgl` into
the package; `test_package/` grows an `hgl_add_module` consumer when the
option is on. This is the channel for teams that already build hgraph
extensions with Conan and want the same lock file to carry the compiler.

### Container image

`language.yml` publishes `ghcr.io/hhenson/hgl:<release>` on tags: an Ubuntu
image with GCC 14, the installed prefix, and the dependency packages, built
from the same configure the formula uses. It serves two things the formula
does not: CI jobs that run `hgl test` on a project, and `hgl run`
deployments of scripted programs, where the "host" is the image.

### PyPI

Unchanged for now. Shipping `hgl` inside the `hgraph` wheel would give
`uv tool install hgraph` a compiler on all three platforms through a
pipeline that already exists, but it is the Python-embedded flavour of
`hgl` (it must link the interpreter, `hgraph_link_python_embedding`), it
adds roughly 30 MB to a wheel, and it needs `language/` in the source
distribution, reversing a Slice 0 acceptance. Revisit when the language
leaves its experimental status.

### Windows

The scripted loader is Unix-only for now, and there is no Homebrew.
Windows users build from the tree or, once the loader lands there, use the
Conan package; winget or scoop manifests can follow the same source
archive later.

## Relocatable native context

The scripted loader proposed in #640 and #641 records, at build time, the
compiler path, the compiler launcher, and the `hgl` target's include
directories, definitions, compile options, and link options, then adds
`<exe>/../include` at run time. On the machine that built `hgl` this
works, and the installed-SDK checks in those pull requests ran there. On
any other machine the recorded paths are somebody else's. The include
list is the transitive closure of the `hgl` target's usage requirements,
so it does name the dependency headers, but as absolute paths of the
build machine: a bottle records the sandbox build tree, Cellar-versioned
dependency directories that change on the next `brew upgrade fmt`, a CI
compiler, and possibly `sccache`; a Conan package records the Conan cache.

The proposal is a native context file that the install step writes and
`hgl` reads relative to its executable, `lib/hgl/native-context.json`:

- **compiler**: a name (`clang++`, `g++-14`) resolved on `PATH` at run time,
  overridden by `HGL_CXX` and then `CXX`; on macOS `xcrun --find clang++`
  is the fallback. No launcher.
- **include and library directories**: the installed prefix, spelled
  relative to the file so the prefix can move, plus the dependency
  prefixes (Arrow, fmt, spdlog, simdjson, date) *as the consuming host
  resolves them*. Homebrew's `opt` paths and the container's `/usr/local`
  are the same on every machine and are recorded as configured. Conan
  cache paths are not: a package is built in one cache and consumed from
  another, so a Conan install records no dependency paths and `hgl` reads
  them from the run environment the recipe's `package_info` computes on
  the consumer from the dependencies' `includedirs`. The file is plain
  text a packager can patch, and a missing prefix falls through to the
  CMake-configure fallback below.
- **flags**: the language standard, PIC, visibility, and the definitions the
  generated code needs (`HGL_HAVE_ANALYTICS`, the API version). Not the
  repository's warning flags: `-Werror` against a compiler the tree was
  never built with is a way to fail a user's program.

When the file is absent (`hgl` run from a build tree), the loader falls
back to the constants it records today. The content-addressed cache (#641)
already keys on toolchain inputs, so a resolved compiler that differs from
the recorded one produces a different entry rather than a stale hit.
`hgl --print-native-context` shows the resolved context so a failed
scripted compile can be diagnosed without reading the cache.

A simpler alternative is to drive every scripted compile through a CMake
configure of a generated one-file project against the installed
`hgraphConfig.cmake`. It is correct by construction and needs no context
file, at a cost of seconds per cold compile; it is the right fallback when
the context file is missing or its compiler cannot be found, and it may be
the right first implementation.

## Deploying HGL programs

Two shapes, matching the two backends in [Architecture](architecture.md):

- **Scripted.** `hgl run app.hgl` on a host that has the `hgraph` package:
  the compiler and the SDK come with it, and runtime functions compile into
  the cache on first use. This is the development, CI, and container
  shape. Its host needs a C++ compiler, so it does not satisfy the release
  gate "deployment artifacts do not require the compiler or source tree at
  runtime".
- **AOT.** A CMake project with `hgl_add_module()` against
  `find_package(hgraph)`, built where the SDK is installed. The result is an
  ordinary native library or executable; its runtime host needs the shared
  SDK libraries of the exact version it was built against (the `hgraph`
  package as a runtime dependency) or a static prefix build. This is the
  shape that satisfies the release gate, and it is unchanged by this
  record; the record only guarantees that the SDK the formula installs is
  the one `hgl_add_module()` documents.

The Python-facing shape, `PYTHON_MODULE` in `hgl_add_module()`, produces a
wheel-shaped package and belongs to the wheel channel, not to this record.

## Work items, in order

1. Release version stamping: `HGRAPH_RELEASE_VERSION`, `version.h`, the
   `hgl --version` line, and the Conan `set_version` tag pattern (core
   CMake, `language/`, `conanfile.py`; `build_system.rst` records it).
2. Offline isocline: honour `FETCHCONTENT_SOURCE_DIR_ISOCLINE` and
   `FETCHCONTENT_FULLY_DISCONNECTED`, or fall back to the plain line reader
   when the fetch is impossible, so a sandboxed build succeeds.
3. Relocatable native context, as above, in the #640/#641 stack or directly
   after it, with an installed-elsewhere test: install to a prefix, move
   the prefix, run a runtime-function test from it.
4. The tap: repository, formula, bottle workflow, and an "Installing"
   section in the user guide's [Modules and tools](../user-guide/modules-and-tools.md)
   naming the formula and the container image.
5. A release-checklist line in `release_readiness.rst`: after a tag, bump
   the formula's `url`/`sha256` and let the tap build bottles.
6. The Conan `language` option and its `test_package` consumer.
7. The container image job in `language.yml`.

## Open decisions

- **Package name.** `hgraph` with an `hgl` alias (this record) or `hgl`
  outright. The alias keeps the SDK discoverable under the project's name
  while letting the tool's name work.
- **Linux bottles.** Building them costs CI minutes on every release; the
  container image may be enough for Linux until someone asks.
- **`hgl` in the wheel.** Deferred above; it is the cheapest way to reach
  Windows and every Python user, at the price of the sdist exclusion.
- **Extension kernels in the package.** Core plus analytics here; kafka,
  web, persistence, and fabric wait for module descriptors and for a
  packaging policy on their own dependencies.
