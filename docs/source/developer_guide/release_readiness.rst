Release readiness
=================

This is the operational release contract for the C++-first ``hgraph 0.8``
line. Version 0.8.0 replaces the Python-first implementation on ``main``; the
0.5 implementation remains available and maintained on ``release/0.5``.
RFC :doc:`../rfc/rfc_0005_hgraph_1_0_api` continues to define the eventual
1.x API freeze.

Compatibility policy
--------------------

- The documented Python ``hgraph`` surface is stable for the ``0.8`` line.
  Private modules and ``_hgraph`` are not public APIs.
- The C++ runtime is the source of truth. Its public source and binary ABI
  remain provisional throughout the ``0.8`` line.
- Every Python-visible runtime behavior must have a native C++ wiring route
  and comparable behavioral coverage. Python-only syntax and opaque Python
  object adaptation are boundary exceptions, not alternate runtime semantics.
- Accepted differences from upstream are listed in :doc:`roadmap`. A new
  difference requires a test and documentation; it must not be hidden by an
  unexplained skip, xfail, or permissive compatibility shim.

Supported platforms
-------------------

.. list-table:: ``0.8`` release targets
   :header-rows: 1
   :widths: 22 28 20 30

   * - Platform
     - Build toolchain
     - Package contract
     - Validation role
   * - Linux x86_64
     - Official ``manylinux_2_28`` image, GCC 14
     - ``cp312-abi3`` wheel, glibc 2.28+
     - Official wheel; Ubuntu 24.04/GCC 14 is the native and performance host
   * - macOS arm64
     - macOS 26 runner, current AppleClang, deployment target 15.0
     - ``cp312-abi3`` wheel, macOS 15+
     - Primary local and CI correctness gate; Intel macOS is not built
   * - Windows x86_64
     - Latest MSVC toolset over Ninja (vcvars via ``msvc-dev-cmd``; the
       Visual Studio generator ignores the sccache launcher)
     - ``cp312-abi3`` wheel
     - Python compatibility suite gates the wheel; standalone native C++ is
       best effort

CPython 3.12, 3.13, and 3.14 install and test the same stable-ABI wheel on all
three wheel platforms. Linux wheel construction is intentionally separate from
the Ubuntu native build so a newer host glibc cannot silently raise the wheel's
compatibility floor.

Migration canaries
------------------

The repository contains three levels of migration evidence:

1. ``python/tests/test_release_readiness.py`` rejects unexplained static skips,
   xfails, and module-wide WIP markers.
2. ``python/tests/ported`` exercises public upstream-compatible behavior,
   including mixed Python/C++ graphs. Behavior tests use ``eval_node``.
3. The native suite proves the equivalent C++ wiring paths and the lower-level
   ownership, type-erasure, nested-graph, and lifecycle contracts.

For each release review, run representative real applications in an environment
containing only the candidate ``hgraph`` wheel, not the installed 0.5
reference distribution. Cover startup and shutdown,
services/adaptors, record/replay, nested map/mesh/switch graphs, Python user
nodes, exceptions, real-time execution, and long-running memory behavior.
Record any incompatibility as a minimal repository test before changing the
implementation.

Release gates
-------------

The definition-of-done commands in ``AGENTS.md`` are authoritative. A release
requires all of the following from a clean checkout:

- fresh Release native configure/build and the complete C++ suite on macOS;
- a Python 3.12 ``cp312-abi3`` wheel, installed into a fresh Python 3.14
  environment, and the complete non-WIP Python suite;
- the same Release gates on Ubuntu 24.04/GCC 14;
- Linux Debug/AddressSanitizer native and Python suites for type-erasure,
  ownership, nested-graph, or bridge-lifetime changes;
- strict ``abi3audit`` for every wheel, install/consumer validation for public
  CMake changes, and a warning-clean build on the gating native toolchains;
- Sphinx ``-W`` documentation, packaging metadata tests, and a source
  distribution inspection; and
- the benchmark correctness guards plus a recorded Linux benchmark run.

GitHub CI is post-push platform evidence, not a substitute for the local gates.
Tagged releases reuse the successful distribution artifacts for the exact
commit SHA and restamp both ``hgraph`` and ``hgraph-kafka`` package metadata to
the shared bare-version tag. For example, ``0.8.1`` publishes both packages as
version ``0.8.1``; prereleases use the same form, such as ``0.8.1rc1``.

Performance evidence
--------------------

``benchmarks/orchestrate.py`` is the cross-implementation benchmark driver and
``python/tests/test_benchmark_scenarios.py`` proves that timed workloads perform
meaningful work. Record comparative results on the Ubuntu 24.04 VM, including
compiler, build type, commit, CPU, sample count, and scaling parameters.
Dynamic TSL scenarios are C++-runtime-only and must not claim a 0.5-reference ratio.
Native microbenchmarks in ``tests/cpp/type_erasure_perf.cpp`` and
``tests/cpp/json_perf.cpp`` protect lower-level dispatch and serialization
costs.

Release review
--------------

The release reviewer should verify:

- the untagged packages use their sentinel versions, both native CMake baseline
  versions agree, and all tagged artifacts have been restamped to the validated
  shared PEP 440 tag version;
- no public Python name relies on a private bridge-only runtime implementation;
- the parity matrix and roadmap contain no stale implemented-as-missing rows;
- every remaining skip has a ``deviation:`` or ``gap:`` reason and an owner or
  accepted policy;
- no compiler warning was suppressed broadly to make a platform green;
- wheel contents exclude build trees, tests, caches, and private development
  artifacts; and
- the validation evidence below names the exact tested commit and is updated
  only after the commands complete.

Historical pre-port validation evidence
---------------------------------------

The following evidence was recorded for the predecessor ``hg_cpp`` package on
21 July 2026. It is historical provenance, not completion evidence for the
0.8.0 port. Runtime and wheel
source was commit ``8ac34dfd6e8f368d45255779329dab8b009d2150``. Commit
``4dff53401d6dcd78bd90b92bffb8c834015e9175`` then extended the distribution
auditor for Linux's standard ``lib64`` install layout; it did not change the
runtime or wheel payload. Commit ``c0582b7e`` completed the matching
``auditwheel`` exclusion set for the three Arrow libraries supplied by
PyArrow; it likewise changed only release automation and its regression test.
Commit ``5086abaa`` then added the linked Arrow Acero runtime to the Windows
wheel after the first post-push import check identified the omitted DLL.

**macOS 26.5.2, arm64, Apple Clang 21.0.0:**

- a fresh Release configure and warning-as-error build passed all 1,203 native
  tests;
- a Python 3.12 stable-ABI wheel passed the 305-file distribution audit and
  strict ``abi3audit``; and
- that wheel, installed into a fresh Python 3.14.6 environment, passed 1,502
  Python tests with 16 documented skips.

**Ubuntu 24.04 VM, x86_64, GCC 13.3.0:**

- a fresh Release configure and warning-as-error build passed all 1,203 native
  tests;
- the local ``cp312-abi3`` wheel passed the 305-file distribution audit and
  strict ``abi3audit``, then passed 1,500 Python 3.14.6 tests with 16 documented
  skips; and
- Debug AddressSanitizer builds passed all 1,203 native tests and all 1,500
  Python runtime tests with 16 skips, without sanitizer diagnostics. Native
  leak detection was enabled; it was disabled for the Python process because
  ownership of interpreter and Arrow shutdown allocations is outside the
  tested runtime.

The wheel built from the same source was also installed temporarily into the
two migration applications without upstream ``hgraph`` present. ``hg_oap`` at
``e4c56c36`` passed 198 tests with 2 skips and 2 expected failures;
``hg_systematic`` at ``522bcd0a`` passed all 39 tests. Both application
environments were restored to their locked release dependencies afterwards.

The controlled Linux core benchmark is recorded in
``benchmarks/results/matrix-20260719-124516.md`` with the corresponding raw
samples in ``raw-20260719-124516.json``. It used three fresh-process samples,
Release/GCC 13, Python 3.12.3, and source fingerprint
``ab42c7aa882b03be53e33de437e4099cf915e6ed558fd7ecad308942dd72c587``.
All workload guards passed. The result preserves both improvements and
regressions relative to upstream instead of treating timings as a release
pass/fail gate.

The macOS installed-package CMake consumer passed against system Arrow 25.
Sphinx 9.1 passed with warnings as errors; all 48 packaging, readiness,
distribution, and benchmark-harness canaries passed; and ``uv lock --check``
was clean. The source distribution contained 735 audited files (2,091,465
bytes), retained the public headers, implementation sources, consumer test,
and audit tool, and excluded benchmark environments/results, release-review
reports, caches, and build trees. Wheel
audits require the extension, native libraries, headers, CMake package,
debugger support, and Python package while accepting the platform-standard
``lib`` or ``lib64`` root.

The official ``manylinux_2_28``/GCC 14 wheel, Windows Visual Studio wheel,
three-version wheel installs, and Linux shared-package consumer all passed in
GitHub Actions run ``29818280267`` at ``5086abaa``. The Windows artifact
included ``arrow.dll``, ``arrow_compute.dll``, and ``arrow_acero.dll`` beside
``_hgraph.pyd``; the same ``cp312-abi3`` wheel passed under Python 3.12, 3.13,
and 3.14. The final compatibility-skip refresh on top of that runtime source
passed 1,204 native tests and 1,509 Python 3.14.6 tests with 10 documented
deviation skips. The final tag review must still name the exact tagged commit.
