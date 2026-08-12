Replacement Gap Plan
====================

This document preserves the completed audit and execution plan that enabled
C++-first hgraph 0.8.0 to replace the Python-first engine on ``main``. It
complements :doc:`roadmap`, which also preserves the validation evidence.

Audit Baseline
--------------

The audit was refreshed on 2026-07-21 against:

- ``hg_cpp`` at ``5086abaa``;
- the adjacent Python hgraph checkout at ``a0deb32e``; and
- application evidence from ``hg_oap`` and ``hg_systematic``, which both run
  against ``hg_cpp``.

The comparison is behavioural and public-API based.  Private Python runtime
objects are not counted as missing when the supported behaviour has a native
C++ route.  In particular, the ``Hg*TypeMetaData`` hierarchy, Python
``GraphBuilder`` / ``WiringNodeInstance`` layouts, implementation-injection
hooks, and generic Python ``nested_graph`` construction remain intentional
non-targets.

The supported replacement surface and release-hardening automation are
complete. Native evaluation tracing, profiling, wiring diagnostics, and owned
graph inspection are implemented. The authoring mismatches found by this audit
were completed in milestones R0 and R1 on 2026-07-20; the remaining operational
milestones completed on 2026-07-21.

The final compatibility-skip audit found that both recorded operator gaps were
stale. Sparse TSB proxy-lag deltas and the complete stream status-message pack
pass unchanged; the former now also has a public C++ ``eval_node`` regression.
The non-WIP compatibility suite contains no remaining ``gap:`` skip.

Completed Authoring Work
------------------------

Milestone R0: upstream canaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Public regression cases now freeze the recent upstream behaviours that were
previously supported only by ad-hoc probes:

- falsey scalar and Arrow ``Frame`` values pass through ``throttle``;
- TSD remove/add/remove and reference invalidation retain the correct
  lifetime;
- ``publish_output(delta=False)`` sends full values instead of deltas;
- mapped invalid bundle fields do not materialize an empty child value; and
- the existing Kafka adaptor cases continue to cover message headers.

The ``Frame`` assertion observes the documented Arrow boundary rather than
requiring a Polars runtime value.  Private Python builder layouts remain
excluded from the compatibility numerator.

Milestone R1: context adaptation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python context syntax now lowers consistently onto the native name-based
context model:

- optional named node contexts retain their declared default when no value is
  published, while required contexts still fail at wiring;
- generic time-series contexts resolve through the native resolution scope,
  including multiple independently named generic contexts;
- graph context parameters are resolved before composition, and an absent
  optional time-series context produces a typed invalid output;
- context lookup covers nested map/switch graphs, service calls, and nominal
  CompoundScalar base types; and
- service and adaptor registration normalize ``None`` to the declaration's
  default path before native binding.

These are adapter changes only.  Context publication, binding, nested graph
execution, and service endpoints remain C++ runtime concepts.

Milestone R1: higher-order overload identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Native C++ ``map_`` and ``mesh_`` overload selection already used semantic
``WiredFn`` identity correctly.  The mismatch was at the Python bridge: it
retained the generated registration wrapper as the callable inspected by
``requires_``, and ``mesh_`` also exposed its private scope-name argument to a
user overload.

The bridge now stores the registration identity, callable wrapper, and
semantic user callable separately.  Private higher-order control arguments
are filtered before a Python overload is invoked.  Paired native and Python
``eval_node`` tests prove both selection for the matching callable and fallback
for a different callable; no Python-owned dispatch path was introduced.

Milestone R1: scalar formatting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ValueOps`` now distinguishes user formatting from diagnostic rendering.
Boolean diagnostics and JSON remain lower-case ``true`` / ``false``, while
``format_`` produces Python-compatible ``True`` / ``False``.  The formatting
operation was appended to the erased operation table and its ABI version was
incremented, with native layout, generic-value, and operator coverage plus a
Python ``eval_node`` case.

Validation Record
-----------------

The completed R0/R1 source was validated on 2026-07-20 with clean builds:

- macOS ARM64 native Release build: 1,164 of 1,164 C++ tests passed;
- macOS ``cp312-abi3`` wheel installed under Python 3.14.6: 1,431 passed and
  16 skipped;
- Ubuntu 24.04 x86_64, GCC 13.3, warnings-as-errors: 1,164 of 1,164 C++ tests
  passed, including the public-header and ABI-boundary targets; and
- Linux ``cp312-abi3`` Release wheel installed under Python 3.14.6: 1,431
  passed and 16 skipped.

Both wheel builds used ``-O3``.  The Linux Python run used
``polars[rtcompat]`` because the x86_64 OrbStack VM does not expose AVX/AVX2.

Operational Gaps
----------------

Milestone R2: profiler and run logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The native ``EvaluationProfiler`` now records graph cycles, wall/evaluation
time, real-time scheduling lag and load, plus per-path start/evaluation/stop
count, failures, total, maximum, and recent-window time. Snapshots own paths
and labels; copies of a profiler share its state so the Python handle remains
readable while the run owns its observer. Explicit lifecycle failure events
finalize start/stop timers without changing the propagated exception.

Graph and node identities are cached during start. Steady evaluation performs
pointer lookups rather than rebuilding paths, and the recent window is a
pre-grown circular vector. With no profiler registered, the observer list is
empty and the runtime performs no profiling clock reads or Python calls.
Process RSS sampling was intentionally not added: it is process-wide and too
expensive at node granularity. Planned graph storage and tracked slot capacity
remain the better input for the later inspector memory view.

``hgraph.test.EvaluationProfiler`` is the bound native observer;
``GraphConfiguration(profile=...)`` accepts an instance, ``True``, or the
upstream options dictionary. Human-readable profile logs format the immutable
snapshot rather than serving as its storage model.

The executor now owns the run's shared spdlog logger. Root and nested graphs
cache the same borrowed pointer, and ``LoggerView`` no longer reads a process
global on the tick path. The Python bridge sink forwards native node, trace,
and runner messages to the configured Python ``graph_logger``; Python-authored
``LOGGER`` parameters receive a guarded facade over the same native
``LoggerView``. Logging no longer retrieves a Python object from copied-in
``GlobalState``.
``default_log_level`` and ``logger_formatter`` apply to mixed graphs, with
node paths supplied through the optional native ``ContextualLogger`` contract.

The allocation/timing guard lives in ``hgraph_type_erasure_perf`` as the
``evaluation_profiler_disabled_cycle`` and
``evaluation_profiler_enabled_cycle`` workloads. Both require zero
steady-state allocations. Exact macOS and Linux measurements are recorded in
the R2 validation record below.

R2 Validation Record
~~~~~~~~~~~~~~~~~~~~

The completed R2 source was validated on 2026-07-20 with clean Release
builds:

- macOS ARM64 native build: 1,170 of 1,170 C++ tests passed;
- macOS ``cp312-abi3`` wheel installed under Python 3.14.6: 1,434 passed and
  16 skipped;
- Ubuntu 24.04 x86_64, GCC 13.3, warnings-as-errors: 1,170 of 1,170 C++ tests
  passed, including the public-header and ABI-boundary targets; and
- Linux ``cp312-abi3`` wheel installed under Python 3.14.6: 1,434 passed and
  16 skipped.

The profiler timing guard used seven samples of 20,000 cycles.  The median
macOS ARM64 cost was 40.094 ns per disabled cycle and 165.331 ns per enabled
cycle.  The median Linux x86_64 cost was 78.764 ns per disabled cycle and
243.382 ns per enabled cycle.  All four measurements reported zero
steady-state allocations.  These are regression baselines, not a
cross-platform speed comparison.

Lifecycle observers and error policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The executor now owns run-wide ``ErrorCaptureOptions`` and cleanup policy.
Uncaught root errors use the same bounded native activation trace as captured
``NodeError`` values. Immediate stop is optional during propagation, but C++
executor destruction remains the mandatory final teardown boundary.

``GraphConfiguration.life_cycle_observers`` and
``eval_node(__observers__=...)`` install observers on the executor's single
native list. Built-in trace/profiler instances stay native; arbitrary Python
observers use an owned adapter and guarded callback-scoped graph/node views.
Root and keyed nested graph lifetimes, push-phase gating, callback expiry,
reentrant native removal, and start/evaluate/stop failure paths have explicit
coverage.

R3 Validation Record
~~~~~~~~~~~~~~~~~~~~

The completed R3 source was validated on 2026-07-20 with clean Release
builds:

- macOS ARM64 native build: 1,173 of 1,173 C++ tests passed;
- macOS ``cp312-abi3`` wheel installed under Python 3.14.6: 1,440 passed and
  16 skipped;
- Ubuntu 24.04 x86_64, GCC 13.3, warnings-as-errors: 1,173 of 1,173 C++ tests
  passed; and
- Linux ``cp312-abi3`` wheel installed under Python 3.14.6: 1,448 passed and
  17 skipped.

Both wheel builds used Release optimisation. The Linux Python environment used
``polars[rtcompat]`` because the x86_64 OrbStack VM does not expose AVX/AVX2.
The six Python lifecycle/error ownership cases also passed against a Linux
Debug extension instrumented with AddressSanitizer; leak detection was disabled
to exclude process-wide Python shutdown state.

Wiring trace
~~~~~~~~~~~~

**Completed.** ``WiringObserver`` and ``WiringTracer`` are native C++ APIs at
the graph composer and operator registry. Events carry stable graph paths,
labels, interned schema handles, selected candidates, effective ranks, and
rejection reasons without exposing Python ``WiringNodeInstance`` objects.
Explicit child-wiring propagation covers higher-order nested compilation.
Observer implementations and event records remain C++-only.
``GraphConfiguration(trace_wiring=...)`` and ``eval_node(__trace_wiring__=...)``
use the native tracer; Python may retain a bound tracer to inspect its formatted
lines but cannot implement an observer callback.

Graph diagnostics
~~~~~~~~~~~~~~~~~

**Completed.** ``GraphDiagnostics`` is a native lifecycle observer. Its owned
snapshots contain stable graph/node identities and hierarchy, schema and
implementation labels, scheduling state, phase timings, root load, fixed graph
plan bytes, and current/peak nested-slot storage. ``NodeOps`` supplies the
cold-path storage measurement, so map, mesh, reduce, ordered reduce, switch,
fixed TSL map, and single nested nodes report storage without an
inspector-side type switch.

The observer forgets runtime pointers at stop/failure boundaries. Historical
records remain queryable after keyed graph delete/erase and executor teardown.
The ``hgraph.debug`` module binds that native observer and provides a flat
snapshot-to-rows helper. The established Python ``inspector()`` API uses the
same owned snapshots for its Perspective/HTTP presentation without
reintroducing the old Python runtime-object walker. Its value-page Arrow
stream is generated from owned JSON or an immutable captured ``Frame`` handle,
and scoped search materialises matches from the owned hierarchy rather than
walking live endpoints.

Catalogue Gaps
--------------

These are useful compatibility additions, but they are lower priority than
the correctness and operational work above.

Arrow dataframe operations
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-21.**  The public ``join``, ``concat``, structural filter,
sort, ``group_by``, ``ungroup``, and ``with_columns`` facades now execute in
C++.  Join uses Arrow Acero's hash join; the other operators use Arrow compute
and the shared native table codec.  Key materialization, compound-item
ungrouping, scalar broadcast, typed ``Series`` columns, output projection, and
group removals have public C++ wiring tests and Python bridge coverage.

The Python wrappers for ``filter_frame(**predicate)`` and
``with_columns(**columns)`` only pack keyword ports into a structural TSB.
The legacy typed third argument to ``ungroup`` is likewise a wiring adapter;
evaluation still delegates to the native operator.  ``TS[Series[T]]`` to
``TS[tuple[T, ...]]`` is native and preserves Arrow nulls as unset tuple
elements.

Expression filters remain supported compatibility APIs.  Their scalar is a
Python ``pyarrow.compute.Expression``, so those specific nodes are the recorded
advanced Python execution path; structural filters with native scalar values
do not use it.

NumPy and analytical facades
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-21.** ``Array[T, Size[...]]`` now retains every dimension
in an interned C++ schema and uses planned fixed-array storage or the compact
dynamic list plan. Generic element and dimension patterns, generic TSB schema
specialization, ndarray conversion, and C++ ``ArrayOf<T, ...>`` authoring use
the same type records.

The complete public ``hgraph.numpy_`` catalogue moved to ``hgraph-analytics``.
``as_array``, ``get_item``, ``cumsum``, and ``corrcoef`` are now
``window_values``, ``array_get_item``, ``cumulative_sum``, and ``correlation``.
The additional ``hgraph.nodes`` exports ``np_rolling_window``, ``np_quantile``,
and ``np_std`` moved as ``rolling_window``, ``quantile``, and ``array_std``.
The obsolete NumPy-derived namespace and prefixes are retired. Core retains
the generic ``rolling_window`` alias; the policy-bearing ``rolling_average``
has moved to ``hgraph_analytics.rolling_mean``. This audit intentionally covers
the published upstream surface even when checked-out applications do not
import it.

Recorded boundaries are numeric ``int``/``float`` kernels, one- or
two-dimensional ``correlation``, fixed tick windows for ``window_values``, and
the five common quantile interpolation methods. Unsupported methods and shapes
fail explicitly. Early ``hgraph_analytics.rolling_window`` output uses dynamic
array dimensions, correcting the old implementation's mismatch between a fixed
shape declaration and shorter warm-up arrays. Quantile and shaped-array
standard deviation follow Arrow Compute; correlation follows Boost.Math, and
cumulative sum uses hgraph's native shaped-array traversal with defined integer
wrapping.

The native ``window`` implementation remains core. The tick/time rolling mean,
running and collection ``std``/``var``, and scheduled ``resample`` contracts
have moved to ``hgraph-analytics`` with their Python compatibility tests. The
earlier roadmap entry claiming rolling support was absent was stale. An
upstream public export remains a migration obligation even when it is not used
by ``hg_oap`` or ``hg_systematic``.

Leaked service and mesh runtime helpers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Accepted implementation boundary (2026-07-21).**  The ``hgraph.nodes``
wildcard export in upstream includes ``capture_output_node_to_global_state``,
``capture_output_to_global_state``, ``get_shared_reference_output``,
``mesh_subscribe_node``, ``request_id``, ``write_service_replies``,
``write_service_request``, and ``write_subscription_key``.  Source review found
that these names are used exclusively by upstream's private Python wiring-node
classes.  They do not form a coherent application authoring API.

The helpers expose the old runtime implementation directly: owning-node
reflection, mutation of captured source nodes, ``GlobalState`` path protocols,
and ``PythonMeshNodeImpl`` graph traversal.  Reproducing them would create a
second, partially assembled service transport beneath the descriptor-based
native API.  In particular, ``capture_output_node_to_global_state`` cannot keep
its stated behaviour without restoring the private ``ts.output.owning_node``
model, and the old per-argument request fan-out differs from the native bundled
request schema.

These accidental exports are therefore intentionally absent.  Supported code
uses ``reference_service``, ``subscription_service``,
``request_reply_service``, adaptors/service adaptors, and ``mesh_`` /
``get_mesh``.  Those paths are C++-owned and have paired C++ and Python
behavioral coverage.  A surface test pins both the supported replacements and
the absence of the leaked helpers.

Deferred And Accepted Restrictions
----------------------------------

The following are not scheduled replacement blockers:

- ``Hg*TypeMetaData`` and the old Python reflection/resolution hierarchy;
- private Python graph-builder, peering, binding, and node-instance layouts;
- a generic Python nested-graph constructor;
- Python ``node_impl`` source decorators where generators and concrete native
  adaptors provide the public authoring route;
- dynamic-TSL mesh, which is an ``hg_cpp`` extension without upstream parity;
- notebook presentation helpers;
- general process checkpoint/recovery beyond component and record/replay
  semantics; and
- durable stores, authentication, deployment, or scheduling policies without
  a concrete application requirement.

Implementation Sequence
-----------------------

Milestone R0: freeze the audit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.**  The public behavioural canaries identified above
are part of the compatibility suite.  Private-runtime collection failures stay
out of the compatibility numerator.  ``hg_oap`` and ``hg_systematic`` remain
release-level application checks rather than repository unit tests.

Milestone R1: authoring correctness
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.**  Context adaptation, default service-path
normalization, semantic higher-order callable identity, and
Python-compatible scalar formatting have paired public native/Python coverage.
The implementation details and validation counts are recorded above.

Milestone R2: native profiler and unified run logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.** Aggregate snapshots, runner phase logging,
run-owned logging, ``profile``, ``graph_logger``, ``default_log_level``,
``logger_formatter``, and the native benchmark guard have paired native and
Python coverage.  The clean macOS/Linux acceptance and performance evidence
is recorded above.

Milestone R3: lifecycle and error configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.** Native lifecycle adapters now back
``GraphConfiguration`` and ``eval_node``; executor-owned error detail and
cleanup policy cover root and nested execution. Callback views expire at the
callback boundary, and failed Python runs with cleanup disabled are retained
by the raised exception until safe final teardown.

Milestone R4: wiring diagnostics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed.** The native event model and ``WiringTracer`` cover graph, nested
graph, node, and operator-resolution wiring. Public C++ tests inspect selected,
rejected, and ambiguous overloads without debugger knowledge of erased
implementation objects; Python tests cover only the configured native tracer.

Milestone R5: native graph diagnostics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.** Inspection snapshots build on lifecycle and graph-view
infrastructure, with native storage reporting through the erased node operation
table. Native and Python ``eval_node`` tests cover switch/map/mesh creation,
key shrink and erase, stopped-state accounting, shared observer copies, and
repeated snapshot queries after teardown.

The final macOS and Ubuntu x86_64 Release gates each passed 1,182 native tests.
Stable-ABI wheels built with Python 3.12 passed 1,461 Python 3.14 compatibility
tests with 17 skips on both platforms. A Linux Debug/ASan extension passed the
Python ``eval_node`` inspector teardown test, and the bindings-off native ASan
build passed all three inspector cases with 120 assertions. Leak detection was
disabled for the Python process-wide shutdown state.

Milestone R6: data and analytics catalogue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-21.** Arrow dataframe
operations, Series conversion, shaped native arrays, and the analytical
extension exports are implemented as described above. The former
``hgraph.numpy_`` and NumPy-prefixed ``hgraph.nodes`` surfaces have migration
mappings and no longer ship in core. Application scans guided additional tests
but did not define or reduce the public compatibility surface. Every Python
facade has a C++ wiring route and paired behavioral coverage.

Fresh macOS and Ubuntu x86_64 Release builds each passed 1,203 native tests.
Stable-ABI wheels built with Python 3.12 each passed 1,493 Python 3.14 tests
with 16 skips. Sphinx passed with warnings as errors, an installed C++ consumer
passed against system Arrow 25, and the Linux Debug/ASan numerical suite passed
8 cases with 128 assertions.

Milestone R7: service and mesh API boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-21.**  Detailed source review reclassified the upstream
``hgraph.nodes`` transport helpers as leaked private-runtime implementation,
not compatibility APIs.  No raw path/schema service transport or owning-node
reflection surface was added.  The supported descriptor-based services,
adaptors, and native mesh access remain covered at the behavioral level in C++
and Python; the Python surface test prevents the internal names from being
introduced accidentally.  The fresh macOS gate passed all 1,203 native tests;
the CPython 3.12 stable-ABI wheel passed 1,494 tests on CPython 3.14 with 16
skips; and the Sphinx build passed with warnings treated as errors.

Milestone R8: release hardening
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-21.**  Release artifacts now have an executable content
audit in local tests and CI. Wheels must contain the stable-ABI extension,
native libraries, public headers, a complete CMake package under ``lib`` or
``lib64``, and debugger support; wheels and sdists reject private, generated,
cached, and unsafe archive paths. The manylinux wheel explicitly compiles with
GCC 14 warnings as errors, and the standalone Linux package installs Arrow
Acero before exercising the shared-library consumer.

The full macOS and Ubuntu Release suites, macOS and Linux stable-ABI wheels,
strict ABI audits, Linux native and Python ASan suites, installed CMake
consumer, and both application canaries passed. :doc:`roadmap`,
:doc:`parity_matrix`, public compatibility documentation, and
:doc:`release_readiness` were refreshed with the exact evidence. The final
Windows Visual Studio wheel, stable-ABI audit, and Python 3.12/3.13/3.14 suites
passed in GitHub Actions at ``5086abaa``. Windows remains a wheel-focused,
best-effort CI gate.

Completion Rules
----------------

Each milestone follows the repository definition of done in ``AGENTS.md``.
Focused tests are iteration evidence only.  Runtime and bridge milestones need
fresh full native and Python 3.14 suites; ownership, observer, inspection, or
cross-language lifetime changes also need the Linux and sanitizer gates.  A
milestone is not complete while an option is accepted and silently ignored:
unsupported options must continue to fail explicitly until their native path
lands.
