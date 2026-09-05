Build System
============

Goals
-----

- CMake is the primary build system.
- The default configure path builds C++ without requiring Python.
- ``pyproject.toml`` drives Python packaging and the optional Python bridge.
- Public C++ consumers should depend on ``hgraph::core``.

Current Targets
---------------

``hgraph_options``
    Interface target for C++ standard, warnings, include paths, sanitizer flags, and common compile definitions.

``hgraph_core``
    Core runtime target. This is exported publicly as ``hgraph::core``.

Third-Party Dependencies
------------------------

Arrow is the native table, series, and numerical-reduction substrate. The
runtime links Arrow, Arrow Compute, and Arrow Acero; wheel builds resolve those
libraries from PyArrow, while standalone C++ builds use their CMake packages.

Boost.Math supplies the correlation kernel used by ``hgraph-analytics``. The
extension uses an installed ``boost_math`` package at version 1.90 or newer
when available, otherwise it fetches the pinned standalone Boost.Math release.
The header-only target is private to the analytics build and does not add a
Boost dependency to installed consumers.

``simdjson`` **requires version 4.5 or newer** — ``json_impl.cpp`` uses
``simdjson::dom::element_type::BIGINT``, which first appeared in 4.5. Wheel
builds (``HGRAPH_BUILD_PYTHON_BINDINGS=ON``) fetch a pinned release (currently
v4.6.4) and link it statically; the default C++ build resolves a system package
via ``find_package(simdjson CONFIG REQUIRED)`` followed by an explicit
``simdjson_VERSION`` check, which rejects older distro packages (Ubuntu 24.04
ships 3.x) at configure time instead of failing mid compile. The check is
explicit rather than a ``find_package`` version argument because simdjson's
package version file uses same-minor compatibility (requesting 4.5 would
reject 4.6.x). The installed ``hgraphConfig.cmake`` carries the same floor.

``HGRAPH_WARNINGS_AS_ERRORS`` applies to this project's targets only;
third-party dependencies such as simdjson build with their own flags and are
not expected to be warning-clean under ours.

Translation-Unit Budget
-----------------------

Compiler memory, not source size, is what bounds build parallelism on the
hosted CI runners (3 cores and 7 GB on macOS arm64; the workflows pass
``--parallel`` explicitly rather than taking Ninja's cores + 2 default).
The operator registration files are the heaviest library translation
units in the tree because each registered overload instantiates its wiring
plan; :doc:`operators` ("Registration translation units") gives the layout
rule that keeps every one of them — the std families and the extension
registrations alike — under **1 GB peak compiler memory** at ``-O3`` with
thin LTO and the private precompiled headers.

Measure against a configured and built tree — the precompiled headers must
exist — with ``tools/measure_tu_cost.py``, which replays each compile
command and records the compiler's peak resident set size::

   cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
       -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
   cmake --build build
   python tools/measure_tu_cost.py build --filter lib/std/operators \
       --budget 1024 --exempt data_frame_impl.cpp

Reference figures (GCC 14, Linux x86_64, ``HGRAPH_BUILD_SHARED=ON``): an
impl header alone costs 0.5–0.7 GB (the Arrow-backed families are the
heavier end) and each registered overload adds 5–8 MB. The single-file
families measured 1.05–1.73 GB before the split; the split registration
files sit between 0.7 and 0.95 GB (``hgraph-analytics``'s
``statistics.cpp`` went from 1.25 GB to three files of at most 0.95 GB).
``data_frame_impl.cpp`` (about 1.1 GB) is Arrow header weight rather than
registrations and is the one documented exception — ``--exempt`` reports it
without failing the run — and it still leaves three parallel jobs well
inside the runner's memory.

The Catch2 suites are heavier than any library unit because every wired
test graph instantiates its plans: ``tests/cpp/test_std_operators.cpp``
peaks at about 2.3 GB, ``test_map.cpp`` at 1.6 GB, and a further eight
suites sit between 1.1 and 1.5 GB (run the tool without ``--filter`` for
the list). They are outside the registration budget — a packaged build
(``BUILD_TESTING=OFF``) never compiles them — and they set the parallelism
of the ``BUILD_TESTING=ON`` workflow jobs: two jobs on the 7 GB macOS
runners, four on the 16 GB Linux runners. Splitting the largest suites by
operator family would let those jobs use every core.

Version Header
--------------

``include/hgraph/version.h`` is generated from ``include/hgraph/version.h.in`` into the CMake build tree. The generated include directory appears before the source include directory so normal includes resolve to the configured header.

The header carries two versions (RFC 0032):

- ``hgraph::version_string`` is the **API version**, ``project(hgraph
  VERSION ...)``. ``hgraphConfigVersion.cmake`` checks it with
  ``SameMajorVersion`` and ``python/tests/test_packaging.py`` pins it.
- ``hgraph::release_version_string`` is the **release version**, the bare
  git tag that ``release_readiness.rst`` makes the version authority. It is
  taken from the ``HGRAPH_RELEASE_VERSION`` cache variable when set;
  otherwise from ``git describe --tags --match "[0-9]*"`` in a git checkout
  (an exact tag gives ``0.8.22``, a later commit ``0.8.22-61-g7b9ebd691``);
  otherwise ``<api>-dev``. Package builds from a release tarball pass it
  explicitly (``-DHGRAPH_RELEASE_VERSION=0.8.23``). ``hgl --version`` and
  the Conan recipe report this version; ``hgraph_smoke_test`` asserts it is
  stamped.

Python Releases
---------------

The Python distribution is published to the ``hgraph`` PyPI project. Version
0.8.0 is the first C++-first release; the Python-first implementation remains
maintained on ``release/0.5`` and is installed separately only as a pinned
compatibility oracle.

``.github/workflows/release-wheels.yml`` builds one ``cp312-abi3`` wheel for Linux x86_64,
Windows x86_64, and Apple Silicon macOS, then installs each platform wheel under
CPython 3.12, 3.13, and 3.14. A bare tag matching ``x.x.x`` publishes the tested
core, Kafka, analytics, web, and persistence wheels and source distributions
through PyPI trusted publishing.
Standalone C++ validation runs independently in
``.github/workflows/native-cpp.yml``. It covers native Linux and macOS builds,
plus a fully optimized Linux shared-library build with IPO, the complete native
test suite, installation, and downstream core, Kafka, analytics, web, and
persistence SDK consumers. This deliberately expensive validation remains visible on pull
requests and ``main`` without delaying or gating publication of wheel artifacts.
The tag is the shared release version authority: the publish jobs restamp the
metadata of artifacts already tested for that exact commit, rather than
rebuilding them. The CMake ``project(VERSION)`` declarations identify the native
API line and are intentionally independent of the shared Python distribution
version. Reused wheels therefore retain the native API version they were built
and tested with; a patch release tag does not require a source commit that only
bumps CMake metadata. The release's numeric core may not predate any native
API version. A prerelease suffix likewise belongs only to the Python distribution
metadata.
``pyproject.toml`` therefore uses ``0.0.0`` as an explicit untagged-artifact
sentinel; it is never the version published to PyPI.  CMake's numeric
``project(VERSION)`` and ``docs/source/conf.py`` track the current C++ API line
independently.  Packaging tests enforce these relationships and the release
workflow validates the tag syntax and rejects a version already present for
any published package on PyPI. Every package the workflow publishes must appear
in ``RELEASE_PACKAGES``; the publish jobs gate on that validation, so a
distribution missing from it would be published unvalidated and a collision
would fail the tag part-way through. The PyPI trusted publishers are bound to the
``release-wheels.yml`` workflow and
the GitHub ``release`` environment.

The macOS build uses the current system Clang from the latest Apple Silicon
runner image while retaining a macOS 15 deployment target.

Downstream Native Extensions
----------------------------

The Python wheel builds ``hgraph_runtime``, ``hgraph_wiring``, ``hgraph_stdlib``,
and the nanobind bridge runtime as shared libraries. The ``_hgraph`` bridge and
downstream native modules must use those same libraries so process-wide type,
operator, plan, and nanobind conversion registries have exactly one instance.
Linking the wheel's runtime statically into another extension is unsupported
because it creates an isolated registry universe that Python wiring cannot see.

The wheel includes public headers and a relocatable ``hgraphConfig.cmake``.
A downstream scikit-build project can add Python's ``purelib`` directory to
``CMAKE_PREFIX_PATH`` and pass its ``lib*/cmake/hgraph`` subdirectories as
``HINTS`` to ``find_package(hgraph CONFIG REQUIRED)``. Explicit hints avoid
depending on CMake's platform-specific ``lib`` versus ``lib64`` prefix
expansion. The project can then use
``hgraph_add_python_module(name STABLE_ABI sources...)`` before linking the
module to ``hgraph::core``. The helper applies nanobind's extension settings
while linking the wheel's shared ``hgraph::nanobind`` runtime; calling
``nanobind_add_module(NB_SHARED)`` directly would create a second runtime.
The SDK config requires the exact nanobind release used to build its shared
runtime (2.13.0 for hgraph 0.8), since nanobind's C++ runtime ABI can change
between releases.
Native modules should be installed beside ``_hgraph``. The helper applies a
relative runtime search path to the wheel's platform-selected library directory
(``lib`` or ``lib64``), using ``@loader_path`` on macOS and ``$ORIGIN`` on ELF
systems.

A native executable linked to the wheel's Python-enabled shared runtime has a
different hosting contract. Extension modules obtain stable-ABI symbols from
the interpreter that imports them, while an executable has no importing
interpreter. Link that executable to its selected Python embedding library with
the installed SDK helper::

   add_executable(application main.cpp)
   target_link_libraries(application PRIVATE hgraph::core)
   hgraph_link_python_embedding(application)

The helper resolves ``Development.Embed`` only when called, retains that
library under ELF ``--as-needed`` linkage, and only links the given executable.
Do not use it for extension modules; doing so would bind an otherwise
stable-ABI module to one Python minor. The application remains responsible for
initializing Python before it executes Python-authored nodes.
The installed ``hgraph::core`` target also carries the Arrow, Compute, and
Acero shared libraries supplied by that environment's compatible ``pyarrow``
installation. Native code can therefore construct ``Frame`` and ``Series``
values through the public Arrow API without relying on indirect DSO linkage;
the same shared Arrow objects remain private implementation dependencies of
the hgraph libraries themselves.

Conan package
-------------

``conanfile.py`` at the repository root packages the shared-library SDK for
Conan 2 consumers: the shared hgraph libraries, public headers, debugger
support, and the project's own ``hgraphConfig.cmake``. Conan does not
generate a competing CMake config (``cmake_find_mode`` is ``none``);
consumers call ``find_package(hgraph CONFIG)`` against the packaged config,
so the SDK helpers (``hgraph_add_python_module``, dependency pinning)
behave identically to a plain CMake install. Dependencies (fmt, spdlog,
simdjson, date/tz, Arrow 25 with compute + acero, all shared) come from
Conan Center; the named-zone backend is pinned to ``date`` for
deterministic behaviour across platforms, and the Python bridge is out of
scope — wheels remain the Python distribution channel. fmt, spdlog and
Arrow are declared with transitive headers and libraries because the
public headers include them (``hgraph::options`` links them for
consumers); simdjson and date/tz are implementation details and stay
private to the package.

Build and test the package locally with::

   CMAKE_POLICY_VERSION_MINIMUM=3.5 conan create . --build=missing \
       -s "arrow/*:compiler.cppstd=gnu20" -s "&:compiler.cppstd=gnu23"

The per-package standards matter: hgraph itself requires C++23, Arrow's
recipe requires (and is only validated at) C++20, and the remaining
dependencies build at the profile default. ``CMAKE_POLICY_VERSION_MINIMUM``
works around transitive recipes (bzip2) whose ``cmake_minimum_required``
predates CMake 4. The version is derived from the latest bare git tag
(the version authority; commits past the tag get a ``.dev<n>`` suffix, and
``pyproject.toml`` is only the no-git fallback) and is passed to the build
as ``HGRAPH_RELEASE_VERSION``; native consumers pin the exact version.
Reachable 0.5 tags from the preserved Python-first history are floored to
the 0.8 line for pre-tag development packages. See the compatibility policy
in :doc:`release_readiness`. ``test_package/`` holds the consumer exercised by
``conan create``. Both the build and the generated config guard the split
``ArrowCompute``/``ArrowAcero`` packages behind target-existence checks
because Conan's Arrow defines all three target namespaces from the single
``Arrow`` config.

The ``language`` option (default off) adds the ``hgl`` toolchain to the
package (RFC 0032): the recipe exports ``language/``, configures with
``HGRAPH_BUILD_LANGUAGE=ON``, fetches the REPL's isocline source in
``source()`` so the configure needs no network, and publishes ``bin`` and
``lib/cmake/hgl``. ``test_package`` then also runs ``hgl --version``::

   conan create . --build=missing -o "hgraph/*:language=True" \
       -s "arrow/*:compiler.cppstd=gnu20" -s "&:compiler.cppstd=gnu23"

First-party extension distributions
-----------------------------------

First-party extensions live under ``extensions/`` in the same repository but
remain separate CMake and Python distributions.  The root
``uv`` workspace makes them selectable without adding an extension dependency
to core ``hgraph``.  With the matching installed hgraph SDK discoverable, an
extension can be built independently, for example::

   CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
     uv build --wheel --package hgraph-kafka --python 3.12

   CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
     uv build --wheel --package hgraph-analytics --python 3.12

   CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
     uv build --wheel --package hgraph-web --python 3.12

   CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
     uv build --wheel --package hgraph-persistence --python 3.12

The Kafka C++ targets can also be included in a repository build with
``HGRAPH_BUILD_KAFKA_EXTENSION=ON``.  That option is off by default: a normal
core configure neither resolves nor links librdkafka.  A standalone native
consumer instead configures ``extensions/kafka`` against the installed
``hgraph`` CMake package and links ``hgraph::kafka``.

The C++-first analytics package follows the same boundary without an external
client library. ``HGRAPH_BUILD_ANALYTICS_EXTENSION=ON`` includes it in a
repository build; standalone consumers find ``hgraph-analytics`` and link
``hgraph::analytics``. Its wheel contains the native library, public headers,
and CMake package as well as the stable-ABI Python registration module, so the
same distribution is usable by C++ and Python authoring environments.

The web and persistence packages have the same shape.
``HGRAPH_BUILD_WEB_EXTENSION=ON`` and ``HGRAPH_BUILD_PERSISTENCE_EXTENSION=ON``
include them in a repository build; standalone consumers find ``hgraph-web`` or
``hgraph-persistence`` and link ``hgraph::web`` or ``hgraph::persistence``. Both
options are off by default, so a normal core configure resolves neither their
dependencies nor their targets.

For persistence that default carries a design rule rather than a convenience:
durable-store policy — Parquet and S3 detection, the recording option
vocabularies — belongs to the extension, so a core configure resolves neither
(RFC 0025). The ``Native C++ / Linux core-only`` leg in ``native-cpp.yml``
builds core with every extension off and fails if the configure resolves
Parquet at all, because the other native legs force the extensions on and
would not notice the dependency coming back.

Each extension owns its nested ``pyproject.toml``, CMake package configuration,
and tests. Cross-cutting changes are tested against core at the same commit,
while the resulting wheels remain separate distribution artifacts. During the
current release line, core, Kafka, analytics, web, and persistence are
co-versioned and co-released: a bare ``<version>`` tag validates that the
version is new for all five packages, restamps their distributions, and
publishes each package. Independent extension tags are intentionally not part
of this release workflow.

The Kafka PEP 517 build requirements include ``hgraph>=0.8.0`` because its standalone
CMake configure consumes the installed core SDK.  In-repository CI installs
the core wheel built from the same commit and builds Kafka without a second
isolated environment; published Kafka source distributions provision the
declared core SDK in their normal isolated build environment.

Open Design Items
-----------------

- Decide when to split runtime, system nodes, schema, and Python bridge into separate targets.
- Decide whether tests should use a bundled test framework or depend on system packages.
- Decide whether the shared extension ABI needs an explicit compatibility
  version independent of the Python distribution version; this is assessed in
  :doc:`extension_policy`.
