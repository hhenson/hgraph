RFC 0032: Native Distribution of hgl and the SDK
================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-04
:Target: release process, native install tree (``hgraphConfig.cmake``),
         ``conanfile.py``, ``language/`` build and CLI, packaging CI
:Related: RFC 0005 (1.0 API and release process),
          ``language/docs/design/distribution.md`` (language-side record)

Summary
-------

Define how the ``hgl`` toolchain and the native hgraph SDK are packaged and
installed **without Python**, so that a user can run
``brew install hhenson/hgraph/hgraph`` (or add a Conan ``requires``, or pull
a container image) and get a working ``hgl`` together with the headers,
libraries and CMake configuration needed to build against hgraph.

The RFC fixes the contract -- artifacts, install layout, versioning, host
requirements, channels -- and splits the work into two sets:

* **Preparation**: everything that can land in this repository and its CI
  without opening a public channel. Version stamping, an offline-capable
  build, the formula and tap templates kept in-repo, a packaging dry-run
  workflow, a build-only container image, the Conan ``language`` option and
  the relocatable native context. None of it changes what a user can
  install today.
* **Release switch**: the short, explicitly triggered sequence that creates
  the public tap, publishes bottles and images, and adds the install
  instructions. It is *not* part of this RFC's implementation; it runs
  when the maintainer decides to expose the language.

Objective
---------

A user on macOS or Linux with no Python installed can:

1. install one package that provides ``hgl`` and the native SDK;
2. run ``hgl check``, ``hgl test``, ``hgl run`` and the REPL on an HGL
   program -- including programs with runtime (``state`` / ``when``)
   functions, which are compiled at run time against the installed SDK;
3. build a C++ program or extension against the installed SDK with
   ``find_package(hgraph)`` and, for language modules, ``hgl_add_module()``
   from the installed ``HglLanguage.cmake``;
4. see one version everywhere: the package version, ``hgl --version`` and
   the codegen banner all report the release tag that produced them.

The maintainer can cut a release by pushing a bare version tag; the native
channels follow from that tag with no edits other than the formula's
``url`` / ``sha256`` bump that Homebrew requires.

Motivation
----------

The only supported install today is the ``hgraph`` wheel. It carries the C++
runtime and the Python bridge but excludes ``language/`` (``pyproject.toml``
``sdist.exclude``), so ``hgl`` is reachable only by configuring this
repository with ``-DHGRAPH_BUILD_LANGUAGE=ON`` and the right dependency
switches. A user who wants to *try* HGL has to install CMake, a C++23
compiler, Arrow and either pyarrow or the fetched third-party sources, and
then find the flags in ``language.yml``.

The pieces of a native channel already exist and are exercised in CI: a
Python-free install prefix (``hgraphConfig.cmake`` with ``find_dependency``
for Arrow, fmt, spdlog, simdjson and date), a Conan recipe for the SDK, and
``language.yml``'s ``cmake --install --component Runtime`` smoke of
``hgl --version``. What is missing is the contract that ties them to a
package manager: a release version the binaries can report, a build that
works inside a sandbox with no network, a formula, and a proof that the
installed tree works when it is not sitting next to the build tree.

Homebrew is the first channel because it is where the request originates
("through brew on mac"), because every dependency except isocline is
already in homebrew-core, and because a formula is the smallest artefact
that gives a user a one-line install. Conan and a container image reuse
the same install tree and cost little more.

Non-goals
---------

* Shipping ``hgl`` inside the ``hgraph`` wheel. Possible later (see
  unresolved questions); it would put Python back on the native path.
* A single static ``hgl`` binary. Arrow, Acero and compute are linked as
  shared libraries and are a formal dependency; the SDK needs the shared
  libraries anyway.
* A Windows native channel. Conan can carry it later; nothing here blocks
  it, but the scripted loader is Unix-only today.
* Submitting the formula to homebrew-core. That is a 1.0 question (stable
  tags, notability); the tap is the channel until then.
* Deploying *HGL programs* as products. ``hgl emit-cpp`` plus the SDK
  covers ahead-of-time builds; how a deployed program is packaged is the
  program's concern, not the toolchain's.

Ownership boundary
------------------

* **hgraph core** owns the release version stamping (``HGRAPH_RELEASE_VERSION``,
  ``hgraph/version.h``), the install tree and ``hgraphConfig.cmake``, the
  Conan recipe, and the in-repo packaging templates under ``packaging/``.
* **The language** (``language/``) owns what ``hgl --version`` prints, the
  offline isocline build, the relocatable native context and the
  ``HglLanguage.cmake`` consumer file.
* **The tap repository** (``hhenson/homebrew-hgraph``, created at the
  release switch) owns nothing normative. It mirrors
  ``packaging/homebrew/`` and adds the bottle blocks that ``brew test-bot``
  writes. Changes to the formula are made here first and copied there.
* **Package managers** own dependencies. The formula, the Conan recipe and
  the Dockerfile never vendor Arrow, fmt, spdlog, simdjson or date; isocline
  is the single exception because no package exists for it.

Contract
--------

Artifacts
~~~~~~~~~

===================== ============================================ ==================================
Artifact              Contents                                     Channel
===================== ============================================ ==================================
``hgraph`` formula    ``hgl`` + SDK from one install prefix        Homebrew tap ``hhenson/hgraph``,
                                                                   alias ``hgl``
``hgraph`` Conan pkg  SDK; ``-o hgraph/*:language=True`` adds      Conan (recipe in-repo; no remote
                      ``hgl``                                      yet)
``hgl`` image         ``hgl``, runtime libraries, a C++ compiler   ``ghcr.io/hhenson/hgl:<release>``
``hgraph`` wheel      Runtime + Python bridge (unchanged)          PyPI
===================== ============================================ ==================================

All native artifacts are produced from the same source tag by the same
CMake configure; a channel differs only in who supplies the dependencies
and where the prefix lands.

Install layout
~~~~~~~~~~~~~~

The install prefix is what ``cmake --install`` produces today with
``HGRAPH_BUILD_LANGUAGE=ON``, ``HGRAPH_BUILD_SHARED=ON`` and Python off,
plus one new file::

    <prefix>/bin/hgl                                   component Runtime
    <prefix>/include/hgraph/...                        public SDK headers
    <prefix>/include/hgraph/version.h                  generated, stamped
    <prefix>/lib/libhgraph_runtime.*                   core libraries
    <prefix>/lib/libhgraph_wiring.*
    <prefix>/lib/libhgraph_stdlib.*
    <prefix>/lib/libhgraph_analytics.*                 analytics kernels
    <prefix>/lib/cmake/hgraph/hgraphConfig.cmake       find_package(hgraph)
    <prefix>/lib/cmake/hgl/HglLanguage.cmake           hgl_add_module()
    <prefix>/lib/cmake/hgl/hgl_python_module.cpp.in
    <prefix>/lib/hgl/native-context.json               NEW (see below)
    <prefix>/share/hgraph/debugger                     debugger helpers

Headers for fetched dependencies (``include/fmt``, ``include/spdlog``,
``include/date``) are installed only when the build fetched them. A
package-manager build sets ``HGRAPH_FETCH_*=OFF`` and installs none of them;
``hgraphConfig.cmake`` then resolves them with ``find_dependency``.

Nothing in the tree bakes an absolute rpath. ``hgl`` carries
``$ORIGIN/../lib`` (``@loader_path/../lib`` on macOS) so it finds the SDK
libraries from any prefix, and the shared core libraries carry ``$ORIGIN``
(``@loader_path``) so each finds its siblings from the directory it was
installed to; both prepend ``CMAKE_INSTALL_RPATH`` when the packager sets
one. The libraries need that rpath of their own because their install names
are ``@rpath/libhgraph_*.dylib`` on macOS and ``DT_RUNPATH`` is not
transitive on ELF: a consumer's rpath reaches ``libhgraph_wiring`` but not
``libhgraph_wiring``'s reference to ``libhgraph_runtime``. Homebrew does not
set ``CMAKE_INSTALL_RPATH`` (``std_cmake_args`` carries no rpath) and leaves
``@rpath`` references alone when it relocates a keg, so the first ``brew
test`` of the installed-SDK consumer failed with ``no LC_RPATH's found``
until the libraries carried the entry themselves.

Versioning
~~~~~~~~~~

There are two versions and they must never be confused:

*API version*
    ``project(hgraph VERSION 0.8.0)`` -- the native API baseline that
    ``hgraphConfigVersion.cmake`` checks with ``SameMajorVersion`` and that
    ``python/tests/test_packaging.py`` pins. Exposed as
    ``hgraph::version_string``. Unchanged by this RFC.

*Release version*
    The bare git tag (``0.8.22``) that ``release_readiness.rst`` already
    makes the version authority for wheels. This RFC makes it visible to
    native builds:

    * A new CMake cache variable ``HGRAPH_RELEASE_VERSION`` (STRING). When
      empty (the default) the configure derives it from
      ``git describe --tags --match "[0-9]*"`` in a git checkout -- an exact
      tag gives ``0.8.22``, a later commit gives ``0.8.22-61-g7b9ebd691`` --
      and falls back to ``<api>-dev`` in a source tree with no git metadata.
      Package builds pass it explicitly (``-DHGRAPH_RELEASE_VERSION=0.8.23``)
      because a release tarball has no ``.git``.
    * ``hgraph/version.h`` gains ``hgraph::release_version_string`` and
      ``hgraph::release_version()``.
    * ``hgl --version`` prints ``hgl <release> (hgraph api <api>)``, for
      example ``hgl 0.8.23 (hgraph api 0.8.0)``. ``hgl`` no longer carries a
      version of its own (``project(hgraph_language VERSION 0.1.0)`` goes);
      the language is part of the hgraph release train.
    * ``hgl emit-cpp`` stamps ``Generated by hgl <release> from ...``.
    * The Conan recipe's ``set_version`` derives from bare tags: an exact
      tag yields ``0.8.23``, commits past a tag yield ``0.8.22.dev61``.

Host requirements
~~~~~~~~~~~~~~~~~

A host that runs an HGL program needs:

* the shared libraries the package manager installs as dependencies:
  Arrow (with compute and Acero), fmt, spdlog, simdjson, date (with the tz
  library and the system time-zone database) and the C++ standard library;
* **for programs with runtime functions only**: a C++23 compiler and the
  installed SDK headers. On macOS the Xcode command-line tools (``clang++``
  via ``xcrun``); on Linux GCC 14 or newer, which the formula declares as a
  dependency. Programs made of compositions and std kernels need no
  compiler.

``HGL_CXX`` overrides the compiler; ``HGL_ARTIFACT_DIR`` overrides where
compiled runtime functions are cached. The cache lives under the user's
cache directory, never inside the install prefix, which is read-only for a
package-manager install.

Relocatable native context
~~~~~~~~~~~~~~~~~~~~~~~~~~

The scripted runtime loader compiles runtime functions with the compiler,
include directories, definitions and flags recorded at *build* time. Those
are absolute build-machine paths (the sandbox build tree, Cellar-versioned
dependency directories, the CI compiler, a compiler launcher) and are wrong
on every other machine. The fix is an install-time context that ``hgl``
reads instead:

* ``cmake --install`` writes ``<prefix>/lib/hgl/native-context.json`` with:
  ``compiler`` (a bare name such as ``c++`` or ``clang++`` -- never the
  build machine's absolute path), ``include_dirs`` (``${prefix}``-relative
  for the SDK; for dependencies, the paths the *installed* SDK resolves
  them at, see below), ``compile_definitions``, ``compile_options`` (no
  warning flags, no launcher, no sanitizers) and ``link_options``.
* Dependency prefixes are a property of the consuming host, not of the
  build. Homebrew's ``opt`` paths and the container's ``/usr/local`` are
  stable across machines and are recorded as they are. Conan's cache paths
  are not: a package is built in one cache and consumed from another, so
  a Conan install records no dependency paths and ``hgl`` takes them from
  the ``package_info`` run environment (``HGRAPH_NATIVE_INCLUDE_DIRS``,
  computed on the consumer from the dependencies' ``includedirs``). If no
  source supplies a dependency prefix, ``hgl`` re-resolves it with a
  CMake configure of a stub project against ``find_package(hgraph)`` and
  caches the answer under ``HGL_ARTIFACT_DIR``.
* ``hgl`` resolves the compiler in this order: ``HGL_CXX``; the context's
  ``compiler`` on ``PATH``; ``CXX``; ``xcrun --find clang++`` on macOS;
  ``c++`` on ``PATH``. The first that exists wins; none found is a
  diagnostic that names the program's runtime functions and what to
  install.
* The build-tree constants remain the fallback when the executable runs
  from the build directory (no ``native-context.json`` beside it), so
  developer flows are unchanged.
* ``hgl --print-native-context`` prints the resolved context for support.
* The installed-elsewhere test: install to prefix A, move it to prefix B,
  run a program with a runtime function with ``HGL_ARTIFACT_DIR`` set to a
  temporary directory and the build tree deleted or unreadable. It passes
  or the package is broken.

The context format is an implementation detail of ``hgl``; only the file's
existence and its ``${prefix}`` relocatability are contractual.

Channels
~~~~~~~~

**Homebrew** (primary). Formula ``hgraph`` in the tap
``hhenson/homebrew-hgraph`` with alias ``hgl``; the source of truth is
``packaging/homebrew/Formula/hgraph.rb`` in this repository. Dependencies:
``boost`` (Boost.Math, header-only, for the analytics kernels), ``cmake``
and ``ninja`` at build time; ``apache-arrow``, ``fmt``,
``howard-hinnant-date``, ``simdjson``, ``spdlog`` at run time; ``gcc`` on
Linux. isocline is a ``resource`` staged into the build directory and
handed to CMake with ``FETCHCONTENT_SOURCE_DIR_ISOCLINE`` under
``FETCHCONTENT_FULLY_DISCONNECTED=ON`` (the Homebrew sandbox has no
network). No formula options. ``packaging/smoke`` is installed as
``share/hgraph/smoke``; the ``test do`` block runs ``hgl test`` on its
program, checks ``hgl --version`` reports the formula version, and builds
``smoke/consumer`` (``find_package(hgraph)`` + ``hgl_add_module()``) with
CMake and Ninja, which are therefore test dependencies as well as build
dependencies.
Bottles are built by ``brew test-bot`` in the tap's own CI on ``macos-26``
arm64 runners, the toolchain the rest of the macOS CI builds with, and
serially: the hosted runners have 3 cores and 7 GB, and the std library's
largest translation units do not fit side by side at ``-O3`` with thin
LTO (the formula passes ``--parallel`` from ``HOMEBREW_MAKE_JOBS``).
Bottles are per macOS version, so macOS 15 builds from source until a
``macos-15`` leg is proven; Linux bottles are an open question.

The formula builds against whatever versions homebrew-core carries (fmt
12.x today, the major the tree also fetches, against a ``find_package``
floor of 11; Boost 1.92 against the 1.90 floor; the Conan recipe pins
fmt 11.2). The packaging dry-run workflow installs the formula itself
from a tarball of the checkout, so a version bump in homebrew-core is
caught in this repository, not by users.

**Conan.** The existing recipe gains ``language`` (default ``False``): it
exports ``language/*``, passes ``HGRAPH_BUILD_LANGUAGE=ON`` and adds
``bin`` to ``bindirs``. ``HGRAPH_RELEASE_VERSION`` is passed from the recipe
version. With the option on, ``test_package`` builds an HGL package
(``hgl_add_module()`` on a copy of the smoke) against the package; because
``hgl`` executes at build time and its shared dependencies live in other
cache entries, the test package activates the run environment for the
build scope as well (``VirtualRunEnv(...).generate(scope="build")``), which
is what a consuming project has to do too. Publishing to a remote is a
release-switch step.

**Container image.** ``packaging/docker/Dockerfile`` builds ``hgl`` on a
Debian (trixie, GCC 14) base. Arrow with compute and Acero comes from the
Apache apt repository. fmt, spdlog, simdjson and date sit below the floors
in ``CMakeLists.txt`` in Debian, so a ``deps`` stage builds them at the
tags the tree would fetch and installs them under ``/usr/local`` in both
the build and the runtime image: the installed ``hgraphConfig.cmake``
calls ``find_dependency`` on each of them, and an image whose SDK cannot
be found by ``find_package(hgraph)`` is not an SDK. Only Boost.Math, a
build-interface-only header dependency, is still fetched. The prefix is
installed to ``/usr/local`` and the final stage keeps ``g++``, CMake,
Ninja and the Arrow ``-dev`` packages so runtime functions and downstream
packages compile against the installed headers; it runs as an unprivileged
user whose home holds the compiled-function cache. CI builds the image on
pull requests touching packaging inputs, runs the smoke inside it and
builds ``packaging/smoke/consumer`` against ``/usr/local``; pushing to
``ghcr.io`` is a release-switch step.

**PyPI.** Unchanged.

**Windows.** Not in scope until the scripted loader supports MSVC or
clang-cl; Conan is the intended vehicle.

Compatibility and migration
---------------------------

* ``hgl --version`` output changes shape (``hgl 0.1.0 (hgraph 0.8.0)`` ->
  ``hgl 0.8.23 (hgraph api 0.8.0)``). ``hgl`` has not shipped, so there is
  no consumer to migrate; the CTest regex is updated in the same change.
* ``hgraph::version_string`` and the API version are untouched; the
  version header only gains symbols. ``test_packaging.py``,
  ``tools/validate_release.py`` and the Sphinx ``release`` check keep
  reading the API version.
* The Conan version scheme moves from ``v_*`` tags to bare tags. The recipe
  has never been published, so no lock file references the old scheme.
* Wheel builds are unchanged; passing the tag to CMake so the Python
  package's native library reports the release version is a follow-up
  (``P7``), not a requirement.
* The formula points at the first tag that contains ``language/``. ``0.8.22``
  predates the language merge, so until the next tag the in-repo formula
  carries a placeholder ``url`` / ``sha256`` and is exercised through the
  dry-run workflow's formula-equivalent build, not through ``brew install``.

Performance and memory
----------------------

Not a runtime change. Build-time notes: a bottle build is a full Release
build of the runtime plus ``hgl`` (~15 minutes on a hosted macOS runner;
Arrow comes from its own bottle). Runtime-function compile latency is
unchanged; the artifact cache (PR #641) keeps second runs free of compiler
invocations and must live in a user-writable directory.

Alternatives considered
-----------------------

* **``pip install hgraph`` provides ``hgl``.** Simplest for Python users and
  the wheel pipeline already exists, but it makes Python a prerequisite for
  a language whose runtime does not need it, and the wheel would carry a
  30 MB binary most Python users never run. Deferred, not rejected.
* **Static single binary.** Ruled out by Arrow (shared, formal dependency)
  and because the SDK -- headers plus shared libraries -- is half the
  deliverable.
* **Vendoring dependencies in the formula.** Against Homebrew policy and
  unnecessary: every dependency but isocline has a bottle.
* **Configure-time CMake in the scripted loader** (generate a mini
  project and let ``find_package(hgraph)`` resolve the toolchain). Robust
  but adds CMake as a runtime dependency and seconds per compile. Kept as
  the fallback design if ``native-context.json`` proves insufficient.
* **A separate ``hgl`` version.** ``0.1.0`` next to hgraph ``0.8.x`` reads
  as two products. One release train is simpler for users, packagers and
  the CI that stamps versions.

Unresolved questions
--------------------

1. **Package name.** ``hgraph`` with an ``hgl`` alias (proposed) keeps the
   SDK discoverable; ``hgl`` as the primary name reads better for language
   users. Decide before the tap is created.
2. **Linux bottles.** ``brew test-bot`` can build them on ``ubuntu``
   runners; whether to pay for that or ship source-only on Linux (and rely
   on the container image) is open.
3. **``hgl`` in the wheel.** See alternatives.
4. **Extension kernels.** Analytics ships inside the one package. Fabric,
   persistence and adaptor kernels have heavier dependencies; whether they
   become separate formulas or stay out of the native channel is open.
5. **macOS code signing.** Homebrew bottles are ad-hoc signed by ``brew``
   on install; notarisation is not needed for a bottle. If ``hgl`` is ever
   distributed as a bare binary outside Homebrew, that changes.

Tasks
-----

Preparation (lands now, exposes nothing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

P1
    Release version stamping: ``HGRAPH_RELEASE_VERSION``,
    ``hgraph::release_version_string``, ``hgl --version`` and the codegen
    banner, the CTest regex, ``build_system.rst``; Conan ``set_version`` on
    bare tags and ``HGRAPH_RELEASE_VERSION`` passed through the recipe.
P2
    Offline isocline: the ``FETCHCONTENT_SOURCE_DIR_ISOCLINE`` +
    ``FETCHCONTENT_FULLY_DISCONNECTED=ON`` path configures and builds,
    documented in ``language/README.md``.
P3
    In-repo packaging material: ``packaging/homebrew/`` (the formula, the
    ``hgl`` alias, the tap's README and its ``test-bot`` / ``pr-pull``
    workflows), ``packaging/docker/Dockerfile``, ``packaging/smoke/``
    (the program and the installed-SDK consumer every channel builds) and
    a ``packaging/README.md`` that spells out the release switch.
    Placeholder ``url`` / ``sha256`` until a tag contains ``language/``.
    The ``hgl`` install rpath gains ``../lib``, the shared core libraries
    gain ``$ORIGIN`` / ``@loader_path``, and both honour
    ``CMAKE_INSTALL_RPATH``.
P4
    Packaging dry-run workflow (``.github/workflows/packaging.yml``): on
    ``workflow_dispatch``, on pull requests touching packaging inputs and
    on ``main``. macOS: ``brew style`` / ``brew audit --strict`` on the
    formula in a throwaway local tap, then the formula is pointed at a
    ``git archive`` of the checkout and installed with
    ``--build-from-source``, followed by ``brew test``, ``brew linkage
    --test``, the smoke and a ``smoke/consumer`` build against
    ``$(brew --prefix)``. Linux: ``conan export``, ``docker build`` of the
    image, then ``hgl --version``, the smoke, the analytics example and a
    ``smoke/consumer`` build inside it. No publishing.
P5
    Conan ``language`` option (exports, ``HGRAPH_BUILD_LANGUAGE``,
    ``bindirs``) and ``test_package`` coverage of ``hgl --version`` and an
    ``hgl_add_module()`` consumer when the option is on.
P6
    Relocatable native context (after PR #641 lands): ``native-context.json``
    at install, compiler resolution order, ``--print-native-context``, and
    the installed-elsewhere test added to ``language.yml``.
P7
    Wheel builds pass ``HGRAPH_RELEASE_VERSION`` from the tag so the Python
    package's native library reports the release version. Optional.

Release switch (explicitly triggered; not part of this RFC's landing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

R1
    Tag a release that contains ``language/`` and passes the packaging
    dry-run.
R2
    Create ``hhenson/homebrew-hgraph`` from ``packaging/homebrew/``
    (``brew tap-new`` layout), set the formula's ``url`` / ``sha256`` to the
    tag, let ``test-bot`` build and upload bottles, merge the bottle block.
R3
    Add the ``ghcr.io`` push to the container job on tags.
R4
    Add the ``language/docs/user-guide`` "Installing" page, the README
    install line, and a release-checklist line in ``release_readiness.rst``
    (bump formula, verify bottles, verify image).
R5
    Mark this RFC Implemented.

Acceptance criteria and test plan
---------------------------------

Preparation is complete when all of the following hold in CI on ``main``:

* ``hgraph_language_cli_version`` passes with the new format, and
  ``hgraph::release_version_string`` is non-empty in every build
  configuration (git checkout, source tarball, explicit override).
* The packaging dry-run workflow is green on macOS with the homebrew-core
  dependency versions of the day, including the offline isocline build,
  and ``brew style`` / ``brew audit --formula`` report no findings for the
  in-repo formula.
* The container image builds, and ``hgl test`` and the
  ``packaging/smoke/consumer`` build pass inside it.
* ``conan export .`` succeeds and ``test_package`` covers the ``language``
  option including the HGL consumer (executed where Conan is available;
  the recipe is not yet in CI).
* After P6: the installed-elsewhere test in ``language.yml`` runs a program
  with a runtime function from a moved prefix with the build tree gone.

The release switch is complete when ``brew install hhenson/hgraph/hgraph``
on a clean machine installs from a bottle, ``brew test hgraph`` passes,
``hgl --version`` reports the tag, and ``docker run ghcr.io/hhenson/hgl:<tag>
hgl --version`` agrees.

Implementation status
---------------------

* 2026-09-04: Proposed. Preparation items P1-P5 land with the branch
  that adds this RFC. Verified locally on Linux: the formula-equivalent
  configure/build/install with FetchContent disconnected, ``hgl 0.8.22
  (hgraph api 0.8.0)`` from the installed prefix and from a copy of it,
  ``hgl test`` on the smoke and on the analytics example from the prefix,
  ``conan export`` / ``conan source`` with ``language=True``. The Homebrew
  install and the container build run only in the dry-run workflow (no
  ``brew`` or ``docker`` on the development box); its first green run
  (2026-09-04) installed the formula from source on ``macos-26`` in eight
  minutes, passed ``brew audit --strict``, ``brew test`` and ``brew
  linkage --test``, and built the Debian image with the smoke and the
  analytics example passing inside it. P6 waits for PR #641
  (native artifact cache), which owns the code the context replaces; P7 is
  optional. No tap, bottle, image or install instructions exist -- the
  release switch has not been pulled.

* 2026-09-04: ``conan create -o "hgraph/*:language=True"`` passes on macOS,
  including compilation and execution of the installed ``hgl_add_module``
  consumer. The full package test found that Conan 2 was pruning the public
  fmt, spdlog, and Arrow usage requirements from consumers of the shared
  package; the recipe now propagates their headers and libraries explicitly.

References
----------

* ``language/docs/design/distribution.md`` -- the language-side record
  (motivation, deployment shapes); defers to this RFC for the contract.
* ``docs/source/developer_guide/build_system.rst`` -- version header,
  Conan package, install tree.
* ``docs/source/developer_guide/release_readiness.rst`` -- tag and restamp
  contract for wheels.
* RFC 0005 -- 1.0 API surface and release process.
* PRs #639, #640, #641 -- scripted runtime loader and artifact cache.
* Homebrew Formula Cookbook and ``brew test-bot`` documentation.
