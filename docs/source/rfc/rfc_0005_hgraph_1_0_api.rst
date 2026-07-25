RFC 0005: hgraph 1.0 API
========================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-25
:Target: hgraph 1.0.0 (PyPI ``hgraph``)

Summary
-------

The C++-first runtime distributed today as ``hg_cpp`` becomes **hgraph 1.0**:
the ``hgraph`` package on PyPI, developed in the ``hhenson/hgraph`` repository.
1.0 is a **clean break** — the ideal API wins, supported by a migration guide,
not by compatibility constraints on the design. The break is staged through
one bridge release: **hg_cpp 0.9** ships the exact proposed 1.0 surface
*alongside* deprecation shims for every renamed or relocated name, and
**hgraph 1.0.0 ships with no compatibility shims at all** — the selected
interface, clean.

The 1.0 shape is a **compact, trusted core** plus an ecosystem of **opt-in
extension packages**. The core distribution contains exactly the language
(types and wiring DSL), the runtime, the operator standard library, the
serialization contracts, the testing harness, and the extension SDK — with
three Python dependencies. Everything that talks to an external system ships
as a separately installed extension distribution. First-party extensions are
developed in the same repository (a monorepo) but built and released as
independent PyPI distributions that plug into the core.

This RFC defines the package structure, the public API surface and its
stability tiers, what constitutes hgraph versus an extension versus a
downstream package, the release and deprecation process for the 1.x line, and
the transition plan for the PyPI name and repository.

Motivation
----------

The upstream roadmap defines 1.0.0 as the release with "agreed and finalised
core requirements to be a valid hgraph implementation", sufficiently decoupled
to allow alternative-language runtimes, and 1.1.0 as the first C++ runtime
release. The ``hg_cpp 0.4`` line has delivered both halves early: the C++
runtime is the source of truth, the documented Python surface is
upstream-compatible (accepted deviations recorded in
:doc:`../developer_guide/parity_matrix`), and the release contract in
:doc:`../developer_guide/release_readiness` has been exercised end to end.
What remains is to make the result *the* hgraph — under the original name,
with a deliberately designed 1.0 contract rather than an accreted one.

The clean break is justified by what 0.x accumulated:

* The upstream package flattens roughly 630 names into ``hgraph.*`` by
  wildcard re-export — accretion, not design. The compatibility bridge already
  proved that a curated surface (162 names plus the operator registry) covers
  the ported test corpus.
* The upstream core installs heavyweight dependencies unconditionally
  (``polars``, ``sqlalchemy``, ``duckdb``, ``pycurl``, ``pytz``, even
  ``black``). The bridge already reduced the core install to three
  dependencies with per-adaptor extras. 1.0 goes one step further and removes
  adaptor *code* from the core distribution as well.
* Versioning to date has been effectively patch-cadence 0.x with no stated
  stability contract. Institutional adoption needs a written promise about
  what may change and when.

A compact core also serves the multi-runtime ambition directly: the smaller
and better specified the core contract, the more realistic a second conformant
implementation becomes.

Design principles
-----------------

1. **Trust concentrates in the core.** The core distribution is small enough
   to audit, has three dependencies, changes conservatively, and carries the
   strongest stability promises. Risk lives at the edges, opt-in.
2. **Extensions are packages, not patches.** An extension extends hgraph
   exclusively through the public extension contract (operator registration,
   scalar registration, services/adaptors, the C++ SDK). If an extension
   needs a private hook, that is a core defect to fix in core.
3. **One repository, many distributions.** First-party extensions are
   co-developed with the core in one repository — atomic cross-cutting
   changes, one CI matrix — but are packaged, versioned, and installed
   independently.
4. **The API is enumerable.** Every public name is either in a curated
   ``__all__``, the enumerated operator registry, or a documented extension
   distribution. If a name cannot be found in the API inventory, it is
   private.
5. **Semantics are versioned deliberately.** Compatibility is promised per
   axis — wiring source, runtime behaviour, serialized data, native ABI —
   because a graph DSL breaks along different axes than a function library.

Ownership boundary
------------------

Three rings, with a one-way dependency direction (application → downstream →
extension → core):

**Core** (the ``hgraph`` distribution)
   The language: time-series types, scalar type system, wiring DSL, and
   generic resolution. The runtime: graph execution, scheduling,
   services/adaptor machinery, error handling, record/replay. The operator
   standard library, including the dependency-free domain operator sets
   (temporal, stream, arrow-combinators, numpy-shaped operators). The
   serialization contracts (JSON, Arrow/Frame/TABLE). The testing harness
   (``eval_node`` and friends). The extension SDK (C++ shared-library SDK and
   the Python registration surfaces). Core dependencies are exactly
   ``frozendict``, ``numpy``, and ``pyarrow``; adding one requires an RFC.

**First-party extensions** (``hgraph-<name>`` distributions, same monorepo)
   Integrations whose purpose is talking to an external system, carrying
   their own third-party dependencies: SQL databases, Kafka, Delta Lake,
   Perspective, web/Tornado, dataframe interop beyond the core Arrow
   contract. Installed explicitly, versioned independently, held to the
   extension contract.

**Downstream packages** (separate repositories)
   Domain-specific values, policies, algorithms, reusable graphs, and private
   integrations — per :doc:`../developer_guide/extension_policy`, which this
   RFC incorporates: incubate downstream, promote through the RFC 0000 gate,
   never import a downstream package from core. The existing
   ``data_catalogue`` adaptor moves downstream at 1.0: it is a young
   pub/sub-catalogue contract that should earn its way back through the
   promotion gate.

The placement rule, in one sentence: **if it defines what hgraph programs
mean, it is core; if it connects hgraph to something else, it is an
extension; if it encodes a domain, it is downstream.**

Distribution and packaging
--------------------------

Compact core distribution
~~~~~~~~~~~~~~~~~~~~~~~~~

The ``hgraph`` distribution ships the core ring only: the ``hgraph`` Python
package, the ``_hgraph`` stable-ABI extension module, the native shared
libraries, public C++ headers, and the CMake SDK package. Runtime
dependencies remain ``frozendict``, ``numpy``, ``pyarrow`` — the floor
established by the 0.4 line, now normative. Wheels are ``cp312-abi3`` for
Linux ``manylinux_2_28`` x86_64, macOS arm64, and Windows x86_64, installed
and tested on CPython 3.12/3.13/3.14, exactly as specified in
:doc:`../developer_guide/release_readiness`.

The adaptor subpackages that ship inside the 0.4 bridge distribution are
dissolved: integrations move to extension distributions; the dependency-free
utility adaptors that are really core runtime conveniences
(``executor``-pool, ``dataclass`` interop, ``run_graph_on_thread``,
async bridging) are re-homed into core modules; ``json`` and ``data_frame``
remain core because they implement the core serialization contracts over
``pyarrow``; ``data_catalogue`` moves downstream.

Extension distributions
~~~~~~~~~~~~~~~~~~~~~~~

First-party extension packages at 1.0:

.. list-table::
   :header-rows: 1
   :widths: 24 26 50

   * - Distribution
     - Import package
     - Contents (0.4 origin)
   * - ``hgraph-sql``
     - ``hgraph_sql``
     - SQL subscriber/adaptors (``adaptors.sql``); ``[snowflake]`` extra for
       the ADBC Snowflake driver
   * - ``hgraph-kafka``
     - ``hgraph_kafka``
     - Kafka publisher/consumer adaptors (``adaptors.kafka``)
   * - ``hgraph-delta``
     - ``hgraph_delta``
     - Delta Lake read/write/query adaptors (``adaptors.delta``)
   * - ``hgraph-perspective``
     - ``hgraph_perspective``
     - Perspective table publication and web view (``adaptors.perspective``)
   * - ``hgraph-web``
     - ``hgraph_web``
     - Tornado-based HTTP/websocket adaptors (``adaptors.tornado``)
   * - ``hgraph-polars``
     - ``hgraph_polars``
     - Polars producers/consumers beyond the core Arrow contract
       (``dataframe`` extra today)

**Naming.** Extension import packages are flat top-level packages
(``hgraph_<name>``), not submodules of ``hgraph``. A dotted namespace under
the core package *is* technically achievable — a regular ``hgraph`` package
can host an init-less PEP 420 namespace child (``hgraph/ext/`` with no
``__init__.py`` in any contributing distribution), and Airflow anchors
``airflow.providers.*`` with a pkgutil-style ``__init__.py`` — but both
flavours share one failure mode: a single distribution that ships a plain
``__init__.py`` into the shared directory silently makes every *other*
extension unimportable, and mixed editable/regular installs are the
documented sharp edge. That cost lands on third-party extension authors'
packaging discipline, which is exactly where a trusted ecosystem should not
place fragility. Flat naming is failure-proof (the Flask precedent:
``flask.ext.*`` was deprecated in favour of ``flask_*`` for the same
reason), keeps imports honest (``import hgraph_sql``), and costs only
aesthetics. Introducing a dotted namespace later would be an additive,
non-breaking change, so choosing flat now forecloses nothing.

**Discovery and registration.** Importing an extension registers its
operators, scalars, services, and adaptors through the public registration
surfaces — exactly as core operator modules register today. There is **no
autoloading**: the core never imports extensions implicitly, and an
un-imported extension contributes nothing (trusted-core principle). Each
extension additionally declares an entry point in the ``hgraph.extensions``
group naming its import package; tooling (the inspector, ``hgraph`` CLI
diagnostics, the API inventory) uses the group to *enumerate* installed
extensions, never to import them behind the user's back.

**Version coupling.** The core never depends on any extension. A pure-Python
extension declares ``hgraph >= 1.N, < 2`` where ``1.N`` is the earliest core
minor providing every contract it uses; the floor is raised on a clock, not
per release (the Airflow providers rule: a minimum core minor may be bumped
once that minor is roughly twelve months old), so extensions are never
forced to chase every core release. An extension containing native code
built against the C++ SDK additionally pins the exact core version it was
built with (``hgraph == 1.N.M``) and is rebuilt per core release, per the
C++ contract below. While an extension is provisional it may carry a
``0.y`` version of its own even though the core is ``1.x`` — the
OpenTelemetry two-track pattern, where the version number itself signals
the tier — and graduates to ``1.x`` without breaking existing users. In the
monorepo, CI builds every extension against the core at HEAD, so drift is
caught before release, not after.

Monorepo and release mechanics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The repository hosts the core and all first-party extensions::

   pyproject.toml            # the hgraph core distribution
   python/hgraph/ ...        # core package
   include/ src/ ...         # native runtime
   extensions/
     sql/pyproject.toml      # hgraph-sql
     sql/hgraph_sql/ ...
     kafka/pyproject.toml    # hgraph-kafka
     ...

Each distribution has its own ``pyproject.toml``, version, and changelog
section; a ``uv`` workspace binds them for development (the Airflow 3.0
pattern: one editable environment, N publishable distributions). Releases
are tag-driven with PyPI Trusted Publishing: ``v_1.2.3`` releases the core;
``sql-v_1.4.0`` releases ``hgraph-sql``. Extension releases are independent
and cheap (pure-Python extensions have no build matrix); a core release is
the expensive artifact and follows the release gates of
:doc:`../developer_guide/release_readiness`. Release automation stays at
JupyterLab scale (a ``jupyter_releaser``-style set of actions: draft
changelog → build changed distributions → publish), explicitly not at
Airflow's release-manager scale — Airflow's ceremony exists for foundation
governance and ninety cloud providers, not for the packaging model itself.
One CI matrix tests core plus all extensions on every change, which is the
monorepo's payoff: a cross-cutting change lands atomically and is proven
against every first-party consumer before anything ships.

Python API contract
-------------------

Module topology
~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 14 56

   * - Module
     - Tier
     - Contents
   * - ``hgraph``
     - stable
     - The curated flat surface: types (``TS``/``TSS``/``TSD``/``TSL``/
       ``TSB``/``TSW``, scalars, schema classes), authoring decorators
       (``graph``, ``compute_node``, ``sink_node``, ``generator``,
       ``operator``, ``component``, ``service_impl``, ...), wiring
       combinators (``map_``, ``reduce``, ``switch_``, ``mesh_``,
       ``feedback``, ...), run/eval entry points, injectable markers,
       services, record/replay, engine constants — plus the operator
       registry surface (below)
   * - ``hgraph.temporal``
     - stable
     - Value-level temporal operations (RFC 0002)
   * - ``hgraph.stream``
     - stable
     - Stream status framework
   * - ``hgraph.arrow``
     - stable
     - Arrow-style combinator DSL
   * - ``hgraph.numpy_ops``
     - stable
     - NumPy-shaped operators (**renamed** from ``numpy_``)
   * - ``hgraph.testing``
     - stable
     - Test harness: ``eval_node``, profiler/trace, wiring tools
       (**renamed** from ``test``)
   * - ``hgraph.reflection``
     - stable
     - Structural decomposition of TS types
   * - ``hgraph.nodes``
     - stable
     - Helper node library
   * - ``hgraph.debug``
     - provisional
     - Inspection snapshots and inspector surface
   * - ``hgraph.notebook``
     - provisional
     - Interactive notebook graph sessions

Renamed modules get an import shim under the old name — emitting a
``DeprecationWarning`` — **in hg_cpp 0.9 only**; 1.0 carries the new names
exclusively. The ``hgraph.adaptors`` namespace exists in 0.9 as warning
shims and does not exist in 1.0.

Public-name rules
~~~~~~~~~~~~~~~~~

* A name is public iff it appears in a public module's ``__all__``, is a
  documented operator-registry name, or is documented in an extension
  distribution. ``from hgraph import *`` yields exactly ``__all__``.
* Underscore-prefixed modules and names are private regardless of
  importability. ``_hgraph`` (the native module) is entirely private; its
  surface may change in any release.
* The **operator registry is a normative public surface**: ``hgraph.<name>``
  resolves any registered operator via module ``__getattr__`` (PEP 562).
  The public registry is enumerated in the API inventory (currently 216
  operators). Registry names with a leading underscore — including the
  dunder-prefixed internal operators (``__py_compute``, ``__harness_record``,
  ...) — are **excluded** from the public contract even though the
  mechanism can resolve them; 1.0 hides them from ``operator_names()``'s
  public enumeration.
* Extensions never inject names into ``hgraph``'s namespace. Their operators
  join the registry under their own names (documented per extension), and
  their Python API lives in their own package.

Stability tiers
~~~~~~~~~~~~~~~

``stable``
   Pinned by the API inventory and its test. Signature or semantic breaks
   only at a MAJOR release, after deprecation.

``provisional``
   Public and documented, marked provisional in the inventory. May change in
   a MINOR release; changes are called out in the changelog and, where
   feasible, shimmed for one minor.

``experimental``
   Gated behind the existing feature-switch machinery
   (``HGRAPH_<FEATURE>`` environment variables / feature configuration
   file). No promises; may vanish.

Whole extension distributions carry a tier the same way (each documents its
own; all first-party extensions start 1.0 as provisional and promote to
stable per-distribution).

C++ contract and SDK
--------------------

The stable deliverable of 1.x is the **Python surface plus the abi3 wheel**,
not the C++ API:

* C++ public headers and the installed SDK are **source-provisional**:
  best-effort source compatibility within a minor line, no promise across
  minors. The C++ ABI is explicitly unstable.
* Native extensions built with the SDK (``hgraphConfig.cmake``,
  ``hgraph_add_python_module``, RFC 0003 scalar registration) pin the exact
  core version and rebuild per core release. The SDK's config should enforce
  the exact-version match at configure time (implementation item).
* A load-time extension ABI identity check — the "Explicit extension ABI
  identity" candidate in :doc:`../developer_guide/extension_policy` — is the
  designated follow-up RFC; until it lands, the exact-version pin is the
  contract.
* A stable C ABI subset is explicitly deferred (unresolved question), not
  promised.
* Because the C++ API has no fixed specification, **no C++ compatibility
  shims exist at any release boundary** — including the 0.9 bridge, whose
  shims are Python-surface only. Renamed or reshaped native surfaces
  simply change; SDK consumers rebuild against the release they target.

Runtime representation and dispatch
-----------------------------------

1.0 makes **no runtime-semantics changes**. Types, operator dispatch,
scheduling, services, error handling, temporal semantics, and the
serialization formats are as fixed by the 0.4 line and RFCs 0001–0004; this
RFC only names, packages, and versions that contract. Behavioural parity
with upstream, including accepted deviations, remains recorded in
:doc:`../developer_guide/parity_matrix`.

Versioning, deprecation, and release process
--------------------------------------------

Compatibility axes
~~~~~~~~~~~~~~~~~~

Semantic versioning for a graph DSL is promised per axis:

**Wiring-source compatibility** (user graph code)
   Stable names, signatures, and wiring-time semantics break only at MAJOR.
   MINOR may add names and change provisional/experimental surfaces. PATCH
   is fixes only.

**Behavioural compatibility** (tick-for-tick evaluation results)
   An intentional behavioural change to a stable operator ships behind a
   feature switch in the minor that introduces it and may become the default
   no earlier than the following minor. Bug fixes that change output to
   match documented semantics are allowed in any release with a changelog
   entry.

**Serialization compatibility** (JSON, Arrow/Frame/TABLE, recordings)
   Additive evolution in minors; readers accept the previous format version
   at a schema-directed boundary (the RFC 0002 v1-ingest pattern is the
   template). Format breaks without a reader are MAJOR.

**Native ABI**
   Outside semver entirely in 1.x (see the C++ contract above).

Deprecation policy
~~~~~~~~~~~~~~~~~~

The mechanism is the existing ``deprecated=`` parameter on the authoring
decorators (warning at wiring time, message carrying the replacement) plus
module-level shims for renames. Policy: a stable name is deprecated for at
least two minor releases or twelve months, whichever is longer, before
removal at the next MAJOR; the warning escalates from
``DeprecationWarning`` to ``FutureWarning`` in the last minor before the
removal release (the pandas PDEP-17 cycle). Provisional names need one
minor of deprecation. Every deprecation lands with its migration-guide
entry in the same change.

Release mechanics and branching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a best-effort project with no formal team: releases happen on
**time and opportunity, not a timed cadence** — this RFC deliberately makes
no schedule commitments. What is committed is the mechanics and the version
semantics. Releases are tag-driven with PyPI Trusted Publishing; ``main``
is always the next release; a ``release/1.N`` branch is cut only when a
past minor needs a fix after ``main`` has moved on. Version increments
follow the compatibility axes above, read practically: **PATCH** for fixes
with no expected impact; **MINOR** for changes with some impact (additive
surface, provisional changes, behavioural switches); **MAJOR** for a
significant break — or for a deliberate milestone, when enough has
accumulated that a new major is the honest signal of significance. A break
of a stable contract *requires* a major; a major does not require a break.
Extension releases are independent of the core's timing. The changelog
keeps the upstream ``Version X.Y.Z`` section convention, one section per
distribution release.

Name and repository transition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **hg_cpp 0.9 — the bridge release**: ships the exact 1.0 surface (new
   module names, extension-distribution split, curated registry) *plus*
   compatibility shims for every 0.4 name that moves or disappears, each
   emitting a ``DeprecationWarning`` naming its replacement. 0.9 is the
   migration vehicle: a codebase that runs warning-free on 0.9 runs
   unchanged on 1.0. No shim crosses into 1.0. Shims are a **Python-only**
   facility: the C++ API has no fixed specification yet (it is
   source-provisional per the C++ contract above), so it carries no
   compatibility layer at any release boundary — native SDK consumers
   track the current headers and rebuild.
2. **Repository**: this codebase merges back into ``hhenson/hgraph``, which
   becomes the monorepo. The merge imports full history (graft/subtree with
   history preserved, approach documented in the migration guide). Trusted
   Publishing supports multiple publisher configurations on one PyPI
   project, so the ``hhenson/hgraph`` workflow is added *before* the old
   configuration is removed — publishing never has a gap. The ``hg_cpp``
   repository is archived with a pointer.
3. **Final upstream 0.x**: one last upstream-lineage release whose project
   description announces 1.0, links the migration guide, and states the
   version fence (users pinning ``hgraph<1`` are unaffected forever).
   Nothing on the 0.x line is yanked: PEP 592 yanking is for defective
   releases, not API-change signalling.
4. **hgraph 1.0.0** publishes from the merged repository once the
   acceptance criteria below hold — preceded by ``1.0.0rc`` pre-releases,
   which installers ignore by default, so 0.x users are untouched until
   they opt in. **1.0 contains no compatibility shims**: it is the selected
   interface exactly, with the 0.9 bridge behind it. Publishing 1.0 on the
   *same* name with an explicit fence is deliberate anti-fragmentation:
   ReactiveX's rename-with-coexistence left three live packages (``Rx``,
   ``rx3``, ``reactivex``) and a split ecosystem.
5. **hg_cpp tombstone**: a final ``hg_cpp 0.10.0`` depends on
   ``hgraph>=1.0`` and raises on import with a message pointing at
   ``hgraph`` (the ``sklearn`` shim precedent, minus the brownout
   theatrics); existing ``hg_cpp 0.4.x``/``0.9.x`` releases are never
   yanked.

Migration from hgraph 0.x and hg_cpp 0.4
----------------------------------------

A migration guide ships with 1.0 as user documentation (this RFC governs its
scope, not its text):

* **From hg_cpp 0.4**: upgrade to the 0.9 bridge, run until warning-free,
  then switch to ``hgraph`` 1.0 unchanged. On 0.9 the
  ``hgraph.test``/``hgraph.numpy_`` names and the
  ``hgraph.adaptors.<name>`` modules all still import — as
  ``DeprecationWarning`` shims naming the replacement (adaptor shims
  re-export the ``hgraph_<name>`` extension when installed and otherwise
  raise with the exact ``pip install hgraph-<name>`` instruction, the csp
  relocation pattern). None of these shims exists in 1.0.
* **From upstream hgraph 0.x**: the ported-test corpus defines the
  compatible surface; the guide enumerates the dropped mega-namespace names
  with their curated replacements, the operator-registry import idiom, and
  the accepted behavioural deviations (from the parity matrix).
* **Recordings and serialized data**: 0.4-format Arrow/JSON recordings are
  readable by 1.0 (RFC 0002 ingest rules); upstream-0.x recordings are out
  of contract.
* Shims are confined by policy to the 0.9 bridge and to the **Python
  surface**: import-path shims and ``deprecated=`` warnings only — no
  behavioural emulation layers, no C++ shims (the native API is
  unspecified and simply moves; SDK consumers rebuild), and nothing
  carried into 1.0. Deprecations *within* the 1.x line follow the
  deprecation policy above; the bridge is the one-time vehicle for the
  clean break itself.

Performance and memory
----------------------

1.0 inherits the 0.4 performance contract unchanged: the release gates and
benchmark evidence of :doc:`../developer_guide/release_readiness` (benchmark
orchestration, workload guards, recorded comparative runs, native
microbenchmarks) carry forward, with the added packaging-level guarantee
that the compact core keeps import cost and dependency weight flat — a
core-import benchmark joins the benchmark set so the "compact" claim is
measured, not asserted.

Downstream incubation and promotion
-----------------------------------

The promotion gate of RFC 0000 and the ownership rules of
:doc:`../developer_guide/extension_policy` apply unchanged, with one
addition: promotion has **two distinct destinations**. A capability may be
promoted from a downstream package into a *first-party extension* (it is
generally useful but still an external-system integration — the normal
case), or into *core* (it defines program meaning — the rare case, held to
the full RFC 0000 gate). A promotion PR must state its destination ring and
satisfy that ring's dependency policy. ``data_catalogue`` is the first
expected round-trip: it exits to a downstream package at 1.0 and may return
through this gate.

Comparative survey
------------------

The packaging and release positions above were tested against comparable
systems (sources in References).

**Wingfoil** (Rust graph stream-processing; Rust crate + Python + TS from
one monorepo) is the counter-example to package splitting: every adapter is
a cargo feature flag in the single core crate, and the Python wheel bakes
in a hand-curated subset of them. That model gives compile-time modularity
with no cross-package pinning — and demonstrates both costs this RFC
avoids: the Python surface is a lagging projection (adapters exist in Rust
but not Python), and adapter dependencies live in the *build matrix* rather
than the dependency graph. Its version history (0.1 to 8.0 in under a
year, no stated stability policy) also illustrates that a compact core
does not by itself buy consumers stability — the written version policy
does.

**point72/csp** (C++ engine + Python DSL) is the closest template and
validates the direction with a lived migration: first-party native
adapters ship in the core wheel, but third-party integrations were moved
out to independently versioned ``csp-adapter-*`` distributions surfaced
through extras — with the old in-tree module kept as a re-export shim that
raises a helpful install error when the extension is absent. csp also
demonstrates the native-ABI hazard this RFC's C++ contract answers: a
release was yanked for an unintended ABI break, out-of-tree Python
consumers pin ``>=X,<1``, and nothing documents the C++ engine as stable.
csp ships per-CPython-version wheels; hgraph's abi3 wheel keeps the
expensive artifact singular per platform.

**Apache Airflow providers** proves monorepo→many-distributions at scale
(~90 providers, independent SemVer per provider, per-provider
``pyproject.toml`` sub-projects bound by a uv workspace since Airflow 3)
and supplies the compatibility policy this RFC adopts (min-core-minor
floors raised on a ~12-month clock; suspension/removal lifecycle). It
equally bounds what a small team should copy: its namespace anchoring
(pkgutil-style ``__init__.py``), constraint-file matrices, and
release-manager CLI exist for foundation governance and cloud-API churn —
the shape transfers, the ceremony does not.

**opentelemetry-python** contributes the two-track stability idea adopted
for provisional extensions: stable ``1.x`` distributions and experimental
``0.y`` distributions coexist in one monorepo, the version number itself
signalling the tier, with a hard rule that graduation must not break
existing users and that experimental packages never outrank stable ones.

**JupyterLab and Dask** frame the versioning choice: JupyterLab runs
independent per-package versions with shared release automation
(``jupyter_releaser``); Dask runs lockstep CalVer across ``dask`` /
``distributed`` because compatibility reasoning was otherwise intractable.
Independent SemVer with declared min-core floors (the Airflow/JupyterLab
side) fits opt-in extensions that must release on their own clock.

**ReactiveX/RxPY** supplies two lessons: the ``rx``→``reactivex`` rename
with unfenced coexistence fragmented the ecosystem into three live
packages — hence this RFC's same-name takeover with an explicit fence —
and the operator-surface history (fluent methods removed for pipe-only,
then restored *additively* in v5) argues for one primary composition
surface designed so alternatives can be added without breakage.
ReactiveX also shows the limit of prose-defined "core": with no
conformance kit, cross-implementation drift set in — motivating the
conformance-test-kit unresolved question.

**pandas and polars** anchor the release policy: pandas' PDEP-17 supplies
the deprecation cycle adopted above (deprecate in minors, escalate
``DeprecationWarning``→``FutureWarning``, remove at major, ≥2 minors'
notice) while its 2.0/3.0 history shows a written cycle disciplines but
does not abolish painful majors; polars demonstrates rapid-cadence SemVer
with explicit "unstable" markers, an exact-version-pinned native plugin
ABI (the same posture as this RFC's SDK rule), and the useful framing that
1.0 is "not sacred" — mistakes remain fixable at 2.0.

**PyPI transition mechanics** (sklearn's tombstone shim, pycryptodome's
and Pillow's renames, PEP 592) inform the transition sequence directly:
yanking is for defective releases only, pre-releases shield 0.x users by
default, tombstones work without brownout theatrics, and Trusted
Publishing permits coexisting publisher configurations during a repository
move.

Alternatives considered
-----------------------

**Batteries-included single distribution** (the csp model; the 0.4 status
quo). One wheel with per-adaptor extras and lazy imports is operationally
simpler — no version matrix, one release. Rejected: it couples the trust
and audit story of the core to every integration's code (installed even
when its dependencies are not), grows the core's release surface with every
adaptor, and gives extension authors a second-class contract (in-tree
adaptors get private hooks; out-of-tree ones do not). The compact-core
model makes first-party extensions eat the same public contract downstream
packages do — the strongest possible test of that contract.

**Dotted extension namespace under the core package**
(``hgraph.adaptors.sql`` provided by a separate distribution). Rejected on
mechanics: ``hgraph`` is a regular package, so PEP 420 namespace merging is
unavailable, and directory-merging tricks (the Airflow approach) require
release tooling and editable-install caveats disproportionate to a small
team.

**Upstream mega-namespace continuation** (~630 flat names). Rejected: the
clean break exists precisely to shed unreviewed surface; the ported corpus
proves the curated surface suffices.

**Dual-package coexistence** (keep publishing both ``hg_cpp`` and
``hgraph``). Rejected: two names for one artifact splits the ecosystem and
doubles the release matrix; the tombstone pattern retires the transitional
name decisively.

**Promising C++ API/ABI stability at 1.0.** Rejected: it would freeze
internals mid-optimization; the abi3 wheel already provides the stable
artifact users consume, and the extension-ABI-identity RFC is the vehicle
for strengthening the native promise deliberately.

Unresolved questions
--------------------

* The extension ABI identity check (load-time diagnostic) — designated
  follow-up RFC per :doc:`../developer_guide/extension_policy`.
* A stable C ABI subset for long-lived native extensions — deferred.
* Whether ``hgraph.notebook`` and ``hgraph.debug`` promote to stable in 1.x
  or move to extension distributions — revisit once 1.0 usage exists.
* The upstream repository's non-ported assets (docs history, issue archive)
  — what carries into the monorepo beyond code history.
* A **conformance test kit**: the upstream roadmap's "valid hgraph
  implementation" goal ultimately wants an executable contract (the
  Reactive Streams TCK model) rather than prose — the ported behavioural
  corpus is the seed; formalizing it as a public compliance suite is left
  to a future RFC.

Acceptance criteria and test plan
---------------------------------

* **API inventory**: ``tools/api_inventory.py`` generates the normative
  inventory (name, module, kind, tier, disposition) from ``__all__`` plus
  the public operator registry; a test asserts the committed inventory
  matches the generated one and that every name carries a tier. The
  dunder-prefixed registry names no longer appear in the public
  enumeration.
* **Packaging**: the monorepo builds ``hgraph`` plus every
  ``hgraph-<name>`` distribution; a fresh environment installing only
  ``hgraph`` imports the core with exactly the three dependencies; each
  extension installs, imports, registers, and runs its behavioural tests
  against the built core wheel.
* **Bridge release**: hg_cpp 0.9 ships the exact 1.0 surface plus warning
  shims for every moved/renamed/removed 0.4 name; a corpus that runs
  warning-free on 0.9 runs unchanged on 1.0 (tested with a representative
  application). hgraph 1.0 contains **zero** shims — verified by the API
  inventory (no shim disposition survives into the 1.0 column).
* **Transition dry run**: the full release flow (core + one extension)
  exercised against TestPyPI from the merged repository, including the
  tombstone build.
* **Migration guide** exists and is linked from the final upstream release
  notes and the tombstone message.
* **Docs**: :doc:`../developer_guide/release_readiness` is updated to the
  1.x contract in the implementation PR that flips this RFC to Accepted.

Implementation status
---------------------

Not started. This RFC is a proposal; implementation follows acceptance per
the RFC 0000 workflow.

References
----------

* :doc:`rfc_0000` — RFC process, promotion gate.
* :doc:`rfc_0001_typed_frame_metadata`, :doc:`rfc_0002_temporal_types`,
  :doc:`rfc_0003_extension_scalar_registration`,
  :doc:`rfc_0004_python_owned_structured_scalars` — the contracts 1.0
  packages.
* :doc:`../developer_guide/extension_policy` — ownership and promotion
  rules incorporated by this RFC.
* :doc:`../developer_guide/release_readiness` — the 0.4 release contract
  carried into 1.x.
* :doc:`../developer_guide/parity_matrix` — accepted upstream deviations.
* Upstream ``ROADMAP.md`` (hhenson/hgraph) — the 1.0.0/1.1.0 definitions
  this RFC fulfils.

Comparative-survey sources:

* Wingfoil: https://github.com/wingfoil-io/wingfoil (feature-flagged
  adapters in ``wingfoil/Cargo.toml``; curated Python subset in
  ``wingfoil-python/README.md``); version history via
  https://crates.io/crates/wingfoil.
* csp: https://github.com/Point72/csp (``pyproject.toml`` wheel matrix and
  extras; ``csp/adapters/slack.py`` re-export shim;
  https://github.com/Point72/csp/releases for the 0.14.1 ABI yank);
  out-of-tree pinning: https://github.com/Point72/csp-gateway.
* Airflow providers: https://airflow.apache.org/docs/apache-airflow-providers/
  (independent versioning, min-Airflow policy, suspension); AIP-8
  https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-8+Split+Providers+into+Separate+Packages+for+Airflow+2.0;
  per-provider sub-projects: https://github.com/apache/airflow/issues/43304.
* opentelemetry-python: https://github.com/open-telemetry/opentelemetry-python;
  two-track versioning:
  https://opentelemetry.io/docs/specs/otel/versioning-and-stability/.
* JupyterLab releaser: https://github.com/jupyter-server/jupyter_releaser;
  Dask lockstep: https://github.com/dask.
* Namespace-package mechanics:
  https://packaging.python.org/guides/packaging-namespace-packages/ and
  https://peps.python.org/pep-0420/.
* RxPY migration: https://rxpy.readthedocs.io/en/latest/migration.html;
  fragmentation: https://github.com/ReactiveX/RxPY/issues/464; Rx contract:
  https://reactivex.io/documentation/operators.html; Reactive Streams TCK:
  https://github.com/reactive-streams/reactive-streams-jvm/blob/master/tck/README.md.
* pandas PDEP-17:
  https://pandas.pydata.org/pdeps/0017-backwards-compatibility-and-deprecation-policy.html;
  optional-dependency policy:
  https://pandas.pydata.org/docs/getting_started/install.html.
* polars versioning and plugin pinning:
  https://docs.pola.rs/development/versioning/ and
  https://docs.pola.rs/user-guide/plugins/expr_plugins/.
* PyPI mechanics: PEP 592 https://peps.python.org/pep-0592/; sklearn
  tombstone https://github.com/scikit-learn/sklearn-pypi-package; Trusted
  Publishing https://docs.pypi.org/trusted-publishers/adding-a-publisher/.

Appendix: 1.0 public API inventory
----------------------------------

The normative inventory is generated, not hand-written: ``tools/api_inventory.py``
(acceptance criteria above) walks ``hgraph.__all__``, the public operator
registry, and each public module's ``__all__``, and emits the checked-in
inventory table (name, module, kind, tier, disposition). The default
disposition is *kept, stable*; this appendix records only the measured
baseline and the exceptions.

Baseline (measured on the 0.4 line at this RFC's creation):

* ``hgraph.__all__``: **162 names** (types, decorators, combinators,
  run/eval, injectables, services, record/replay, temporal values,
  constants).
* Operator registry: **216 names**, of which **~14 underscore/dunder
  internals** (``__py_compute``, ``__harness_record``, ``__json_object``,
  ...) are excluded from the public enumeration at 1.0.
* Public modules: ``temporal``, ``stream``, ``arrow``, ``numpy_``→
  ``numpy_ops``, ``test``→``testing``, ``reflection``, ``nodes``,
  ``debug``, ``notebook`` (tiers per the topology table).

Dispositions other than *kept, stable*:

.. list-table::
   :header-rows: 1
   :widths: 34 20 46

   * - 0.4 surface
     - 1.0 disposition
     - Notes
   * - ``hgraph.test``
     - renamed → ``hgraph.testing``
     - warning shim in hg_cpp 0.9; absent in 1.0
   * - ``hgraph.numpy_``
     - renamed → ``hgraph.numpy_ops``
     - warning shim in hg_cpp 0.9; absent in 1.0
   * - ``hgraph.adaptors.sql`` / ``.kafka`` / ``.delta`` /
       ``.perspective`` / ``.tornado``
     - moved → ``hgraph_<name>`` extension distributions
     - re-export warning shims in hg_cpp 0.9; absent in 1.0
   * - ``hgraph.adaptors.data_catalogue``
     - moved downstream
     - re-enters via the promotion gate when proven
   * - ``hgraph.adaptors.data_frame`` / ``.json``
     - re-homed in core
     - implement core serialization contracts (pyarrow only)
   * - ``hgraph.adaptors.executor`` / ``.dataclass`` /
       ``.run_graph_on_thread`` / ``._async``
     - re-homed in core
     - dependency-free runtime conveniences, not integrations
   * - dunder registry operators
     - privatized
     - removed from the public operator enumeration
   * - ``hgraph.debug``, ``hgraph.notebook``
     - kept, provisional
     - promotion decision deferred (unresolved questions)
