RFC 0005: hgraph 1.0 API
========================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-25
:Updated: 2026-08-08
:Target: hgraph 1.0.0 (PyPI ``hgraph``)

Summary
-------

The C++-first runtime entered the ``hgraph`` package and
``hhenson/hgraph`` repository at version 0.8.0. This RFC now governs the later
**hgraph 1.0** API freeze. 1.0 remains a **clean break** — the ideal API wins,
supported by a migration guide rather than permanent compatibility constraints
on the design. The repository and package-name transition described in the
original proposal was completed earlier than its proposed 0.9 bridge release;
the Python-first implementation remains maintained on ``release/0.5``.

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
   standard library, including the dependency-free kernel operator sets
   (temporal, stream — the kernel calibration in the Python API contract
   defines the boundary). The
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
   * - ``hgraph-analytics``
     - ``hgraph_analytics``
     - Statistical estimators and ``Array``-shaped analytics moved out of
       the kernel (0.4 ``numpy_`` module + scalar statistics operators)
   * - ``hgraph-compose``
     - ``hgraph_compose``
     - The composition-combinator DSL (0.4 ``hgraph.arrow``, renamed)
   * - ``hgraph-notebook``
     - ``hgraph_notebook``
     - Interactive notebook graph sessions (0.4 ``hgraph.notebook``)

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

Kernel calibration
~~~~~~~~~~~~~~~~~~

The core operator library is sized like a language runtime's standard
library, not like a framework: the calibration reference is the C standard
library and the C++ STL — a compact kernel of operations that are *truly
necessary to write anything useful*, with everything opinionated left to
libraries. The evidence base:

* ``libc`` exposes on the order of a dozen headers of everyday operations
  (arithmetic in the language, ``math.h``, ``string.h``, ``time.h``); the
  C++ ``<algorithm>``/``<numeric>`` libraries define roughly a hundred
  generic algorithms — and famously contain **no statistics**: mean,
  variance, and correlation live in domain libraries, not the kernel.
* ReactiveX's canonical operator catalogue is roughly seventy operators in
  ten categories (creation, transformation, filtering, combination, error
  handling, utility, conditional, aggregation, connectable, conversion) —
  and the categories map almost one-to-one onto the hgraph kernel families
  below.
* csp's in-tree ``baselib`` follows the same instinct: a small
  flow-control/conversion/timing kernel, with analytics (``csp.stats``)
  and integrations layered on top.

Measured against that bar, the 0.4 surface is close but not clean: the
kernel families below (arithmetic, comparison, string, flow control,
conversion, collections, temporal, serialization, lifecycle) are exactly
stdlib-shaped, while the statistical estimators and array analytics that
accreted alongside them (``ewma``, ``corrcoef``, ``quantile``, ``std``,
...) are precisely what the STL leaves out of the kernel. 1.0 draws that
line explicitly: they move to an extension. Testing support, by contrast,
is deliberately **in** the core — the test harness is what gives users the
direction and tooling to build correct graphs, and a kernel you cannot
verify against is not compact, it is merely small. Logging likewise stays
core: emitting diagnostics is generic to any application, so the logging
operators and a native implementation are part of the kernel.

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
     - The curated flat surface and operator registry (enumerated name by
       name below)
   * - ``hgraph.temporal``
     - stable
     - Value-level temporal operations (RFC 0002)
   * - ``hgraph.stream``
     - stable
     - Stream status framework (``Data``/``Stream``/``StreamStatus`` and
       status-aware combine/merge/reduce) — the interop contract extension
       adaptors build on
   * - ``hgraph.testing``
     - stable
     - Test harness: ``eval_node``, evaluation profiler/trace, wiring
       tools (**renamed** from ``test``)
   * - ``hgraph.reflection``
     - stable
     - Structural decomposition of TS types (``fields``, ``scalar_type``,
       ``element_type``, predicates)
   * - ``hgraph.nodes``
     - stable
     - Helper node library (collection/window conveniences without
       external dependencies)
   * - ``hgraph.debug``
     - provisional
     - Inspection snapshots and inspector surface

Moved out of core (see the extension table and the moves list below):
``hgraph.numpy_`` (analytics extension, renamed — the implementation is
hgraph's own ``Array`` type, not NumPy, so the NumPy-derived name goes
with the move), ``hgraph.arrow`` (composition-combinator extension,
renamed to shed the Apache-Arrow name collision), ``hgraph.notebook``
(a specific use case of the engine, not kernel).

Renamed modules get an import shim under the old name — emitting a
``DeprecationWarning`` — **in a designated pre-1.0 bridge release only**; 1.0 carries the new names
exclusively. The ``hgraph.adaptors`` namespace exists in that bridge as warning
shims and does not exist in 1.0.

The 1.0 surface, name by name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The tables below are the proposed contract: every public name with its
behaviour in one line. Names are grouped where one description covers the
family; a ``(0.4: name)`` annotation marks a rename; unannotated names are
unchanged from 0.4. The generated API inventory (appendix) is the
machine-checked form of these tables.

**Types and schema**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``TS``, ``TSS``, ``TSD``, ``TSL``, ``TSB``, ``TSW``
     - The time-series types: scalar value, set, keyed dictionary, fixed
       list, bundle, and window over a schema
   * - ``TimeSeries``, ``Graph``, ``Node``
     - Runtime handles for a wired time series, graph, and node
   * - ``TimeSeriesSchema``, ``CompoundScalar``, ``Size``, ``REF``
     - Bundle schema base, structured scalar base, TSL size marker, and
       reference-typed time series
   * - ``Frame``, ``TABLE``, ``TableSchema``, ``ToTableMode``,
       ``make_table_schema``, ``table_schema``, ``table_shape``,
       ``table_shape_from_schema``, ``shape_of_table_type``
     - Arrow-backed frame type and the TABLE tuple-row protocol (RFC 0001)
   * - ``Array``
     - The native fixed/variable-shape array scalar (operators over it
       live in the analytics extension)
   * - ``SCALAR``, ``NUMBER``, ``NUMBER_2``, ``COMPOUND_SCALAR``,
       ``COMPOUND_SCALAR_1``, ``SCHEMA``, ``K``, ``V``, ``OUT``
     - Wiring type variables for generic operator signatures
   * - ``CivilDateTime``, ``Period``, ``ZoneId``, ``ZonedDateTime``,
       ``InstantRange``, ``InstantRangeSet``, ``CivilDateRange``,
       ``CivilDateRangeSet``, ``MonthEndPolicy``, ``AmbiguousTimePolicy``,
       ``NonexistentTimePolicy``, ``Boundary``
     - The RFC 0002 temporal value types and policies
   * - ``CmpResult``, ``DivideByZero``, ``RecordReplayEnum``
     - Result/policy enums used by comparison, division, and record/replay
       operators
   * - ``NodeError``, ``TryExceptResult``, ``TryExceptTsdMapResult``
     - Error-capture value types produced by ``exception_time_series`` /
       ``try_except``
   * - ``REMOVE``, ``REMOVE_IF_EXISTS``
     - Delta sentinels marking key removal in TSD/TSS ticks (strict and
       tolerant forms)
   * - ``MIN_ST``, ``MIN_DT``, ``MIN_TD``, ``MAX_DT``, ``MAX_ET``
     - Engine time constants (start/never-modified sentinel/tick quantum/
       idle/end)
   * - ``IN_MEMORY``, ``IN_MEMORY_DENSE``, ``DATA_FRAME``
     - Record/replay storage-mode constants
   * - ``WiringError``, ``ParseError``, ``IncorrectTypeBinding``,
       ``RequirementsNotMetWiringError``
     - Wiring-time error hierarchy
   * - ``WiringPort``, ``MeshWiringPort``, ``WiringGraphContext``,
       ``WiringNodeSignature``, ``WiringNodeType``
     - Wiring-time introspection: ports, graph context, and node
       signature/kind descriptors

**Authoring and wiring**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``graph``, ``compute_node``, ``sink_node``, ``generator``,
       ``push_queue``
     - Authoring decorators: composition graph, computed node, terminal
       node, pull-source generator, and thread-safe push source
   * - ``operator``, ``dispatch_``, ``component``
     - Overloadable operator declaration, runtime value dispatch, and
       recordable component graphs
   * - ``map_``, ``reduce``, ``switch_``, ``mesh_``
     - Higher-order wiring: keyed mapping, associative reduction, sampled
       branch selection, and on-demand instance meshes
   * - ``feedback``, ``delayed_binding``
     - Cycle-breaking: next-cycle feedback and late-bound ports
   * - ``lift``, ``lower``
     - Move ordinary functions into the graph and graphs into callables
   * - ``pass_through``, ``no_key``, ``passive``, ``REQUIRED``,
       ``default_path``
     - Wiring argument markers (multiplex pass-through, key exclusion,
       passive inputs, required-input marker, default service path)
   * - ``try_except``, ``exception_time_series``
     - Error capture: wrap a sub-graph / expose a node's error stream
   * - ``downcast_ref``, ``get_mesh``, ``context``, ``get_context``
     - Reference downcast, mesh handle lookup, and context scoping
       (publish/consume wiring-scoped ports)

**Run, evaluate, and observe**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``run_graph``, ``GraphConfiguration``, ``EvaluationMode``
     - **The single way to run a graph** in simulation or real time:
       ``run_graph(graph, *args, config=None, **kwargs)`` accepts either a
       ``GraphConfiguration`` *or* the individual run keywords (which
       build one — passing both is an error); the 0.4 dunder keywords
       (``__trace__``, ``__profile__``, ...) become ordinary
       ``GraphConfiguration`` fields
   * - ``evaluate_const``
     - Evaluate a const-foldable expression at wiring time (unrelated to
       running a graph)

   * - ``EvaluationLifeCycleObserver``
     - Lifecycle hook interface for graph/node start/stop/evaluate events
   * - ``EvaluationClock``, ``EvaluationEngineApi``
     - Injectable engine clock and engine-control API (request stop, ...)
   * - ``STATE``, ``SCHEDULER``, ``CLOCK``, ``LOGGER``, ``NODE``,
       ``CONTEXT``, ``TSB_OUT``, ``TSD_OUT``, ``TSS_OUT``, ``TSW_OUT``
     - Injectable markers for node state, scheduling, clock, logging, the
       node handle, context, and typed output views
   * - ``GlobalState``, ``GlobalContext``
     - Process/global key-value state seedable at wiring and readable at
       runtime
   * - ``is_feature_enabled``
     - Feature-switch query (the experimental-tier gate)
   * - ``utc_now``
     - Engine-consistent wall-clock now

0.4 exposed two run entry points — ``run_graph`` (keyword convenience)
and ``evaluate_graph`` (config-object form). 1.0 keeps exactly one:
``run_graph``, which absorbs the config-object calling style, so the
cleanliness that motivated ``evaluate_graph`` survives without the
duplicate. The name choice follows both the ecosystem and hgraph's own
vocabulary: the cross-framework verb for whole-lifecycle execution is
*run* (``csp.run``, ``bytewax.run``, Beam ``pipeline.run()``), while in
hgraph *evaluate* names what the engine does once per cycle
(``EvaluationMode``, ``EvaluationClock``, ``eval_node``) — a graph is
evaluated many times in one run, so "evaluate the graph" is the wrong
description of the whole. ``evaluate_graph`` is a 0.9 warning shim and
absent in 1.0.

**Services and adaptors (the extension wiring contract)**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``reference_service``, ``subscription_service``,
       ``request_reply_service``
     - The three service flavours (shared output, keyed subscription,
       request/reply)
   * - ``service_impl``, ``register_service``, ``get_service_inputs``,
       ``set_service_output``, ``impl_input``, ``impl_output``
     - Implement and register services; access flavour inputs/outputs from
       an implementation
   * - ``adaptor``, ``adaptor_impl``, ``service_adaptor``,
       ``service_adaptor_impl``, ``register_adaptor``
     - Adaptor interfaces (graph↔external exchange) and per-client keyed
       service adaptors
   * - ``from_graph``, ``to_graph``
     - Impl-side ports of an adaptor interface; accept an ordinary
       ``client_id=`` keyword to split one adaptor client across calls
   * - ``service_client_id`` (new; 0.4: ``request_id`` +
       ``__request_id__=``)
     - Allocate the process-unique client token used to correlate a split
       service-adaptor client — replaces the generic-sounding top-level
       ``request_id`` operator and its dunder keyword
   * - ``register_native_scalar_type``, ``register_python_object_type``
     - The RFC 0003/0004 scalar registration surface for extensions

**Record, replay, and state**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``record_replay_scope``, ``RecordReplayContext``,
       ``set_record_replay_config``
     - Scope and configure record/replay for a run
   * - ``get_recorded_value``, ``set_recorder_api``, ``get_recorder_api``,
       ``set_recording_label``, ``get_recording_label``
     - Access recordings and bind recorder implementations/labels
   * - ``set_as_of``, ``get_table_schema_date_key``,
       ``set_table_schema_date_key``, ``get_table_schema_as_of_key``,
       ``set_table_schema_as_of_key``
     - Bitemporal as-of and table key configuration
   * - ``frame_metadata``, ``with_frame_metadata``,
       ``without_frame_metadata``, ``has_frame_metadata``,
       ``frame_store_contains``, ``frame_store_read``
     - RFC 0001 frame-metadata accessors and the frame store
   * - ``set_time_zone_provider``
     - Install the named-zone provider for a run (RFC 0002)

**Operator kernel — arithmetic and math**
(``libc``/STL analogues; element-wise over ticking values)

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``add_``, ``sub_``, ``mul_``, ``div_``, ``floordiv_``, ``mod_``,
       ``divmod_``, ``pow_``
     - Binary arithmetic with the documented division-by-zero policy
   * - ``neg_``, ``pos_``, ``abs_``, ``sign``, ``round_``, ``clip``
     - Unary numeric operations and clamping
   * - ``ln``
     - Natural logarithm (see the new-surface list for ``exp_``/``sqrt_``)
   * - ``min_``, ``max_``, ``sum_``, ``count``, ``mean``, ``zero``
     - Running fold aggregations over a ticking value or collection, and
       the additive-identity source used by ``reduce``
   * - ``diff``
     - Difference between consecutive ticks (STL
       ``adjacent_difference``)

**Operator kernel — comparison and logic**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``eq_``, ``ne_``, ``lt_``, ``le_``, ``gt_``, ``ge_``
     - Element-wise comparisons
   * - ``cmp_``
     - Three-way comparison producing ``CmpResult``
   * - ``and_``, ``or_``, ``not_``, ``all_``, ``any_``
     - Boolean logic and quantifiers over collections
   * - ``bit_and``, ``bit_or``, ``bit_xor``, ``invert_``, ``lshift_``,
       ``rshift_``
     - Bitwise operations

**Operator kernel — string**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``str_``, ``format_``
     - Stringify a value; format with a template
   * - ``concat``, ``join``, ``split``, ``substr``, ``replace``,
       ``match_``
     - String manipulation and regular-expression matching (see the
       new-surface list for case/trim completions)

**Operator kernel — flow control and timing**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``const``, ``nothing``, ``default``
     - Constant source, never-ticking source, and fallback-until-valid
   * - ``filter_``, ``if_``, ``if_true``, ``if_then_else``, ``if_cmp``,
       ``route_by_index``
     - Conditional gating and routing of ticks
   * - ``merge``, ``race``
     - One selection family, two selector conditions: ``merge`` forwards
       the first input to *tick* each cycle (with ``disjoint=True`` as a
       wiring-time parameter for non-overlapping TSD keys); ``race``
       forwards the first input to be *valid*, holding until it
       invalidates. Input shape (variadic, TSL, TSD entries) is
       overload-selected — no shape-specific public names
   * - ``sample``, ``dedup``, ``drop_dups``, ``lag``, ``gate``, ``batch``,
       ``throttle``, ``drop``, ``take``, ``step``, ``until_true``
     - Sampling, duplicate suppression, tick delay, flow gating, batching,
       rate limiting, and prefix/suffix stream slicing
   * - ``to_window``
     - Convert a stream into a ``TSW`` window of the last ``period`` ticks,
       valid once ``min_window_period`` ticks have arrived (the sole
       windowing entry point; the legacy bundle-shaped ``window`` is
       retired — see demotions)
   * - ``schedule``
     - Periodic/alarm tick source (wall-clock capable in real time)
   * - ``modified``, ``valid``, ``last_modified_time``,
       ``last_modified_date``, ``last_modified_wall_clock_time``,
       ``evaluation_time_in_range``
     - Tick/validity observation and evaluation-time predicates
   * - ``stop_engine``
     - Request an orderly engine stop from within the graph
   * - ``assert_``, ``debug_print``, ``print_``, ``log_``, ``null_sink``
     - Diagnostics kernel: assertion, debug/console output, logging
       through the native logger, and a swallow-everything sink

**Operator kernel — conversion and structure**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``convert``, ``combine``, ``collect``, ``emit``
     - Generic type conversion; build structures from parts (the target
       type selects the kernel: ``combine[TS[MyCS]](...)``,
       ``combine[TSD[...]]``, ...); accumulate ticks into collections;
       emit collection elements as ticks
   * - ``explode``, ``freeze``
     - Expand a collection tick into keyed ticks; snapshot a collection
       into its frozen scalar
   * - ``apply``, ``call``
     - Apply a wired callable / call a value-level callable per tick
   * - ``type_``, ``downcast_``
     - Runtime type observation and checked downcast
   * - ``getattr_``, ``setattr_``, ``getitem_``, ``get_item``, ``slice_``,
       ``index_of``, ``len_``, ``is_empty``, ``contains_``, ``sorted_``
     - Structural access and interrogation over ticking values and
       collections

**Operator kernel — keyed and set collections**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``keys_``, ``values_``, ``rekey``, ``flip``, ``flip_keys``,
       ``collapse_keys``, ``uncollapse_keys``
     - TSD key/value views and key-structure transforms
   * - ``group_by``, ``ungroup``, ``partition``, ``unpartition``
     - Grouping and partitioning of keyed streams
   * - ``union``, ``intersection``, ``difference``,
       ``symmetric_difference``
     - Set algebra over TSS (and set-valued ticks)
   * - ``make_tsd``, ``filter_tsd_by_matches``
     - Build a TSD from parts; filter by key matches
   * - ``min_ts_list``, ``max_ts_list``
     - Element-wise min/max across a TSL

**Operator kernel — temporal and calendar** (RFC 0002)

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``at_zone``, ``convert_zone``, ``resolve_civil``, ``to_instant``,
       ``to_civil``
     - Zone resolution and instant↔civil conversion via the installed
       provider
   * - ``temporal_floor``, ``temporal_ceil``, ``temporal_round``,
       ``temporal_bucket``
     - Quantize instants/durations; bucket an instant into its interval
   * - ``range_contains``, ``range_intersection``, ``range_overlaps``,
       ``range_touches``, ``range_adjacent``, ``range_mergeable``,
       ``range_difference``, ``range_union``, ``range_merge``,
       ``range_hull``, ``range_shift``, ``range_extent``
     - The normalized range algebra over instant/civil-date ranges
   * - ``day``, ``month``, ``year``, ``weekday``, ``isoweekday``,
       ``day_of_month``, ``month_of_year``, ``isoformat``
     - Calendar field extraction and ISO formatting

**Operator kernel — serialization and persistence**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``to_json``, ``from_json``, ``json_encode``, ``json_decode``,
       ``json_as_bool``, ``json_as_int``, ``json_as_float``,
       ``json_as_str``
     - Schema-directed JSON conversion and JSON-value access
   * - ``to_table``, ``from_table``, ``from_table_const``
     - The TABLE tuple-row protocol over graph values
   * - ``to_data_frame``, ``from_data_frame``, ``filter_frame``,
       ``with_columns``
     - Frame construction, filtering, and column derivation over the core
       Arrow contract
   * - ``record``, ``replay``, ``replay_const``
     - Record a stream / replay a recording as a source
   * - ``record_compare`` (0.4: ``compare``)
     - Backtesting comparison sink: records per-tick equality of two
       streams through the frame store (record/replay COMPARE mode);
       renamed to end the collision with the three-way comparison
       ``cmp_``

Demotions from the flat namespace
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following 0.4 ``__all__`` members are implementation or convenience
surface, not contract, and leave the flat namespace at 1.0 (0.9 keeps them
with a warning):

* ``date``, ``datetime``, ``time``, ``timedelta`` — re-exports of the
  standard library; users import these from ``datetime``.
* ``WiringNodeClass``, ``PythonWiringNodeClass``,
  ``PythonGeneratorWiringNodeClass``, ``OperatorWiringNodeClass``,
  ``GraphWiringNodeClass`` — concrete wiring implementation classes; the
  public introspection story is ``WiringNodeSignature``/``WiringNodeType``
  and ``hgraph.reflection``.
* ``extract_kwargs``, ``extract_signature``, ``equal_lambdas``,
  ``comparison_summary``, ``pass_through_node`` — internal helpers that
  leaked into the surface (``pass_through`` the operator remains).
* ``evaluate_graph`` — folded into ``run_graph``, which now accepts a
  ``GraphConfiguration`` directly (rationale under *Run, evaluate, and
  observe*).
* ``window`` — the legacy bundle-shaped trailing window (buffer +
  timestamps), superseded by the first-class ``TSW`` produced by
  ``to_window``; one windowing surface remains.
* ``compare`` — renamed ``record_compare`` (it is the record/replay
  comparison sink, not a comparison operator; the old name collided with
  ``cmp_``).
* ``combine_cs``, ``combine_map``, ``combine_tsd``,
  ``combine_tss_from_tsl``, ``combine_json``, ``filter_cs`` — erased
  dispatch kernels that the ``combine``/``filter_`` wiring sugar targets
  internally; they are implementation, not contract, and move to
  underscore-prefixed registry names (joining the excluded internal
  operators). The public surface is ``combine`` subscripted by its target
  type.
* ``wire`` — the string-named erased invocation primitive the sugar is
  built on (``wire("combine_tsd", ...)``); implementation, not contract.
  Dynamic by-name invocation of *public* operators remains supported
  through the registry itself — ``getattr(hgraph, name)(...)`` — without
  exposing the erased contract's resolution knobs.
* ``request_id`` — the client-token allocator behind service adaptors;
  privatized as an operator. Its one public use (splitting an adaptor
  client across ``from_graph``/``to_graph``) gets the first-class
  ``service_client_id`` + ``client_id=`` surface in the services table,
  retiring the ``__request_id__`` dunder keyword.
* ``reduce_tsd_with_race``, ``reduce_tsd_of_bundles_with_race``,
  ``merge_tsd_disjoint`` — shape specializations of the ``merge``/``race``
  selection family (upstream keeps the race forms in its ``_impl`` package;
  upstream's own ``merge`` docstring frames disjointness as a
  ``disjoint=True`` parameter). Per the rfc_0000 policy-selector rule,
  wiring-time overload resolution selects these kernels; the public verbs
  are ``merge`` and ``race`` alone, and the kernels move to underscore
  registry names.

Surface moved out of core
~~~~~~~~~~~~~~~~~~~~~~~~~

Applying the kernel calibration, the following leaves the core
distribution for the analytics extension (``hgraph-analytics``, import
``hgraph_analytics``): the statistical estimators ``std``, ``var``,
``ewma``, ``corrcoef``, ``quantile``, ``pct_change``,
``rolling_average``, ``resample``, and the ``Array``-shaped operators
formerly in ``hgraph.numpy_`` (``as_array``, ``cumsum``, ``np_std`` →
``array_std``, ``rolling_window_arrays``). (``mean`` stays in core as a
plain fold beside ``sum_``/``count``.) These need domain policy (estimator
parameters, windowing conventions) that the kernel deliberately does not
own — the same line the STL draws. The ``Array`` *type* remains core; only
the analytics over it move. The NumPy-derived module name is retired with
the move: the implementation is hgraph's native array machinery, and the
extension's names say what they are rather than what they resemble.

``hgraph.arrow`` (the composition-combinator DSL) moves to
``hgraph-compose`` (import ``hgraph_compose``): expressive but not
necessary, and its 0.4 name collides with Apache Arrow — which core
otherwise uses to mean columnar data — so the move takes the rename with
it. ``hgraph.notebook`` moves to ``hgraph-notebook``: a specific
interactive use case of the engine, cleanly built on public surface.

New surface
~~~~~~~~~~~

Gaps identified while enumerating the contract; each ships in 0.9 marked
**new** (provisional until proven):

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Name(s)
     - Behaviour
   * - ``hgraph.__version__``
     - The installed distribution version (currently absent; standard
       expectation)
   * - ``hgraph.extensions``
     - Enumerate installed extension distributions via the
       ``hgraph.extensions`` entry-point group (tooling/inspector support;
       never imports)
   * - ``configure_logging``
     - Bind the native (spdlog-backed) runtime logger from Python: level,
       sink, format — the implementation half of the core logging story
       (``log_``/``LOGGER`` are the operator half)
   * - ``exp_``, ``sqrt_``
     - Complete the kernel math family alongside ``ln``/``pow_``
       (``math.h`` calibration); the wider transcendental set stays out
       until demonstrated
   * - ``lower_``, ``upper_``, ``strip_``
     - Complete the kernel string family (case conversion and trimming)
   * - ``hgraph.testing.assert_ticks``
     - Compare a recorded output against an expected tick sequence with
       aligned, readable failure diffs (today's tests hand-roll list
       comparison)

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

1. **hgraph 0.8.0 — completed repository and package transition**: the
   C++-first tree replaces ``main`` through a history-preserving merge and is
   published under the existing ``hgraph`` distribution name. The C++ API
   remains source-provisional, so native SDK consumers track the current
   headers and rebuild.
2. **Python-first maintenance**: ``release/0.5`` retains the final
   Python-first implementation and receives maintenance releases independently
   of C++-first ``main``. Existing releases and tags are never yanked.
3. **Pre-1.0 evolution**: the 0.8 line may retain compatibility names while
   the proposed compact 1.0 surface is proven. Any later warning bridge is a
   separate release decision; this RFC does not retroactively require the
   superseded ``hg_cpp 0.9`` sequence.
4. **hgraph 1.0.0** publishes from the merged repository once the acceptance
   criteria below hold, preceded by ``1.0.0rc`` pre-releases. **1.0 contains
   no compatibility shims**: it is the selected interface exactly.
5. **hg_cpp archival**: existing ``hg_cpp`` releases remain available as
   historical artifacts. A tombstone distribution, if useful, is handled as
   a separate release and must point users to ``hgraph>=0.8``.

Migration from hgraph 0.5 and hg_cpp 0.4
----------------------------------------

A migration guide ships with 1.0 as user documentation (this RFC governs its
scope, not its text):

* **From hg_cpp 0.4**: replace the distribution requirement with
  ``hgraph>=0.8``. The public import package remains ``hgraph``; separately
  distributed adaptors must be installed explicitly where documented.
* **From Python-first hgraph 0.5**: the ported-test corpus defines the
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
* Whether ``hgraph.debug`` promotes to stable in 1.x or moves to an
  extension distribution — revisit once 1.0 usage exists.
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
* **Bridge release**: a designated hgraph 0.x release ships the exact 1.0
  surface plus warning shims for every moved/renamed/removed compatibility
  name; a corpus that runs warning-free there runs unchanged on 1.0 (tested with a representative
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
* Public modules: core ``temporal``, ``stream``, ``test``→``testing``,
  ``reflection``, ``nodes``, ``debug``; moved out: ``numpy_`` (analytics
  extension), ``arrow`` (compose extension), ``notebook`` (extension) —
  tiers per the topology table.

Dispositions other than *kept, stable*:

.. list-table::
   :header-rows: 1
   :widths: 34 20 46

   * - 0.4 surface
     - 1.0 disposition
     - Notes
   * - ``hgraph.test``
     - renamed → ``hgraph.testing``
     - warning shim in the pre-1.0 bridge; absent in 1.0
   * - ``hgraph.numpy_``
     - moved → ``hgraph-analytics`` (with the scalar statistics operators)
     - warning shim in the pre-1.0 bridge; absent in 1.0; ``Array`` type stays core
   * - ``hgraph.arrow``
     - moved → ``hgraph-compose``
     - renamed with the move (Apache Arrow collision); 0.9 shim only
   * - ``hgraph.notebook``
     - moved → ``hgraph-notebook``
     - 0.9 shim only
   * - stdlib re-exports (``date``/``datetime``/``time``/``timedelta``),
       concrete ``*WiringNodeClass`` classes, ``extract_kwargs``/
       ``extract_signature``/``equal_lambdas``/``comparison_summary``/
       ``pass_through_node``
     - demoted from ``__all__``
     - warning in 0.9; private (or reflection-hosted) in 1.0
   * - ``evaluate_graph``
     - folded into ``run_graph(config=...)``
     - warning shim in 0.9; absent in 1.0
   * - ``window``
     - retired (superseded by ``to_window`` → ``TSW``)
     - warning shim in 0.9; absent in 1.0
   * - ``compare``
     - renamed → ``record_compare``
     - warning shim in 0.9; absent in 1.0
   * - ``combine_cs`` / ``combine_map`` / ``combine_tsd`` /
       ``combine_tss_from_tsl`` / ``combine_json`` / ``filter_cs``
     - privatized (underscore registry names; dispatch kernels of
       ``combine``/``filter_``)
     - public names warn in 0.9; underscore-only in 1.0
   * - ``wire``
     - privatized (erased invocation primitive; use
       ``getattr(hgraph, name)`` for dynamic invocation)
     - warning in 0.9; private ``_wire`` in 1.0
   * - ``request_id`` / ``__request_id__=``
     - replaced → ``service_client_id`` + ``client_id=`` keyword
     - warning shims in 0.9; absent in 1.0
   * - ``reduce_tsd_with_race`` / ``reduce_tsd_of_bundles_with_race`` /
       ``merge_tsd_disjoint``
     - privatized (``merge``/``race`` shape specializations;
       overload-selected, disjointness via ``merge(..., disjoint=True)``)
     - public names warn in 0.9; underscore-only in 1.0
   * - ``hgraph.adaptors.sql`` / ``.kafka`` / ``.delta`` /
       ``.perspective`` / ``.tornado``
     - moved → ``hgraph_<name>`` extension distributions
     - re-export warning shims in the pre-1.0 bridge; absent in 1.0
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
   * - ``hgraph.debug``
     - kept, provisional
     - promotion decision deferred (unresolved questions)
   * - ``exp_``, ``sqrt_``, ``lower_``, ``upper_``, ``strip_``,
       ``configure_logging``, ``hgraph.__version__``,
       ``hgraph.extensions``, ``testing.assert_ticks``
     - new, provisional
     - kernel completions and tooling surface (see New surface)
