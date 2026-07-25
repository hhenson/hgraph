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

Boost.Math supplies the correlation kernel used by ``hgraph.numpy_``. CMake
uses an installed ``boost_math`` package at version 1.90 or newer when
available, otherwise it fetches the pinned standalone Boost.Math release. The
header-only target is private to the stdlib build and does not add a Boost
dependency to installed hgraph consumers.

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

Version Header
--------------

``include/hgraph/version.h`` is generated from ``include/hgraph/version.h.in`` into the CMake build tree. The generated include directory appears before the source include directory so normal includes resolve to the configured header.

Python Releases
---------------

The preview Python distribution is published to the existing ``hg_cpp`` PyPI
project while it is validated independently of the main ``hgraph``
distribution.  It installs the ``hgraph`` Python package and ``_hgraph`` native
extension, so the two distributions should be tested in separate environments.

``.github/workflows/build.yml`` builds one ``cp312-abi3`` wheel for Linux x86_64,
Windows x86_64, and Apple Silicon macOS, then installs each platform wheel under
CPython 3.12, 3.13, and 3.14.  A tag matching ``v_x.x.x`` publishes the tested
wheels and source distribution through PyPI trusted publishing.  The tag is the
release version authority: the publish job restamps the metadata of artifacts
already tested for that exact commit, rather than rebuilding them.  The version
in ``pyproject.toml`` and ``docs/source/conf.py`` is the untagged artifact
baseline.  CMake's ``project(VERSION)`` field is numeric and matches that
baseline's base version (for example ``0.4.0rc1`` maps to ``0.4.0``).  Packaging
tests enforce the baseline relationships and the release workflow validates
the tag syntax and rejects versions already present on PyPI.  The PyPI trusted
publisher is bound to the ``build.yml`` workflow and the GitHub ``release``
environment.

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
runtime (2.13.0 for hgraph 0.4.2), since nanobind's C++ runtime ABI can change
between releases.
Native modules should be installed beside ``_hgraph``. The helper applies a
relative runtime search path to the wheel's platform-selected library directory
(``lib`` or ``lib64``), using ``@loader_path`` on macOS and ``$ORIGIN`` on ELF
systems.

Conan package
-------------

``conanfile.py`` at the repository root packages the shared-library SDK for
Conan 2 consumers: the shared hgraph libraries, public headers, debugger
support, and the project's own ``hgraphConfig.cmake``. Conan does not
generate a competing CMake config (``cmake_find_mode`` is ``none``);
consumers call ``find_package(hgraph CONFIG)`` against the packaged config,
so the SDK helpers (``hgraph_add_python_module``, dependency pinning)
behave identically to a plain CMake install. Dependencies (fmt, spdlog,
simdjson, date/tz, Arrow 24 with compute + acero, all shared) come from
Conan Center; the named-zone backend is pinned to ``date`` for
deterministic behaviour across platforms, and the Python bridge is out of
scope — wheels remain the Python distribution channel.

Build and test the package locally with::

   CMAKE_POLICY_VERSION_MINIMUM=3.5 conan create . --build=missing \
       -s "arrow/*:compiler.cppstd=gnu20" -s "&:compiler.cppstd=gnu23"

The per-package standards matter: hgraph itself requires C++23, Arrow's
recipe requires (and is only validated at) C++20, and the remaining
dependencies build at the profile default. ``CMAKE_POLICY_VERSION_MINIMUM``
works around transitive recipes (bzip2) whose ``cmake_minimum_required``
predates CMake 4. The version is derived from the latest ``v_`` git tag
(the version authority; commits past the tag get a ``.dev<n>`` suffix, and
``pyproject.toml`` is only the no-git fallback); native consumers pin the
exact version, per the compatibility policy in :doc:`release_readiness`. ``test_package/`` holds the consumer exercised by
``conan create``. Both the build and the generated config guard the split
``ArrowCompute``/``ArrowAcero`` packages behind target-existence checks
because Conan's Arrow defines all three target namespaces from the single
``Arrow`` config.

Open Design Items
-----------------

- Decide when to split runtime, system nodes, schema, and Python bridge into separate targets.
- Decide whether tests should use a bundled test framework or depend on system packages.
- Decide whether the shared extension ABI needs an explicit compatibility
  version independent of the Python distribution version; this is assessed in
  :doc:`extension_policy`.
