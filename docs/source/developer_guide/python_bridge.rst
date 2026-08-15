Python Bridge Internals
=======================

This page is the design record for the *implementation* of the Python bridge:
how the code is organised, and — more importantly — the non-obvious invariants
that make it correct. Its companion, :doc:`python_integration`, records the
*behavioural* contract (what the bridge supports and why); this page records
where things live and the "why is it like this?" answers that are otherwise
scattered across inline comments. If something in the bridge looks wrong,
check the table at the end before "fixing" it.

Two layers, one direction
-------------------------

The bridge is two artifacts:

- ``_hgraph`` — the nanobind extension module (sources in ``python/*.cpp`` /
  ``python/*.h``), a thin, *erased* projection of the C++ runtime: type
  handles, wiring verbs, runtime views, and the Python user-node trampolines.
- ``hgraph`` — the pure-Python package (``python/hgraph/``), which reproduces
  the hgraph authoring surface (decorators, type subscripts, operator sugar,
  the test harness) *over* ``_hgraph``.

Direction matters (ruling 2026-07-06, recorded in the memory corpus and
:doc:`parity_matrix`): **the C++ API is primary**. Anything exposed to Python
must be C++-clean first; the Python layer adapts calling conventions, it never
holds runtime state of its own beyond wiring-time bookkeeping. The erased
contract is the load-bearing idea: Python never sees templates, only
registry-resolved operators addressed by name and schema.

Module map — C++ side (``python/``)
-----------------------------------

``module.cpp`` is a thin orchestrator: module init (exception translator,
leak-warning policy, slot wiring, standard-operator registration), the
``bind_*`` calls, and the reset entry points. Everything else lives in domain
files:

============================ ==================================================
File                          Contents
============================ ==================================================
``module_internal.h``         ``PyObj`` (the object-scalar carrier) + the
                              value-conversion API shared with
                              ``value_conversion.cpp``.
``py_carriers.h``             Small cross-TU carrier structs handed to/from
                              Python (``PyTsType``, ``PyValueType``, patterns,
                              ``PyPort``, ``PyNodeRef``/``PyNodeRecord``,
                              ``PySender``, ``PyServiceDesc``, switch/dispatch
                              cases, feedback) and their ``std::hash`` /
                              ``scalar_name`` specializations.
``py_runtime.h``              The runtime view structs user nodes touch during
                              evaluation (``PyTimeSeries``, ``PyOutput``,
                              ``PyStateRef``, ``PyScheduler``, ``PyEvalClock``,
                              ``PyRecordableState``, ``PyRuntimeGlobalState``).
``py_wiring.h/.cpp``          ``PyWiring``/``PyRun`` (wiring + run loop entry),
                              the leaked graph-fn/WiredFn/node registries, and
                              ``bind_wiring()`` (Wiring/Run classes, node_ref,
                              graph_fn, switch/dispatch/feedback, component).
``py_nodes.cpp``              The Python user-node machinery: call shape,
                              input activity, lifecycle assembly, the
                              compute/sink/generator trampolines, harness
                              replay/record, and ``register_python_overloads()``
                              (the single registration list used by module init
                              AND ``reset_registries`` — keep it single).
``py_type_system.cpp``        ``bind_type_system()``: type constructors and
                              introspection, patterns, ``ResolutionScope``,
                              generic-target resolution, and
                              ``register_python_overload``.
``py_ports.cpp``              ``bind_ports()``: ``Port``, port tags, the
                              TSB/TSL packing block (``tsb_port``/``tsl_port``/
                              ``bundle_port``), ``TimeSeriesRef``.
``py_state_services.cpp``     ``bind_state_and_services()``: ``_GlobalState``,
                              record/replay config, services/adaptors/context,
                              the runtime view bindings, Arrow in/out, enum and
                              sentinel slot setters.
``py_bindings.h``             The ``bind_*`` / ``register_python_overloads``
                              declarations ``module.cpp`` calls.
``value_conversion.cpp``      Value ↔ Python object conversion, bound onto the
                              ops tables (see *No kind-switches* below).
============================ ==================================================

All promoted types live in ``namespace hgraph::python_bridge``. Do **not**
move a type back into an anonymous namespace if it crosses a translation-unit
boundary: nanobind identifies bound types by ``std::type_index``, and
anonymous-namespace types have per-TU identity — the failure mode is
"type not registered" at *runtime*, not a link error.

Module map — Python side (``python/hgraph/``)
---------------------------------------------

The wiring layer lives in the ``_wiring/`` package (the historical
``_runtime.py`` module is gone; ``hgraph/__init__.py`` re-exports the public
surface, internal consumers — including the C++ side's import of the
global-state push/pop hooks — address ``hgraph._wiring`` directly):

============================ ==================================================
Module                        Contents
============================ ==================================================
``_wiring/_core.py``          The wiring stack (``_wiring_stack`` — THE single
                              list) and top-level wiring lock, ``WiringPort`` + all attached sugar,
                              ``wire()``, ``_OperatorFunction``, the wiring
                              error hierarchy, context publication.
``_wiring/_sentinels.py``     ``REMOVED``/``Removed``/``_SetDelta`` and delta
                              simplification (identities handed to C++ at
                              import — define once, only re-export).
``_wiring/_state.py``         ``GlobalState``/``GlobalContext``, record/replay
                              config and scopes, the runtime global-state
                              push/pop hooks called from C++.
``_wiring/_markers.py``       Injectable markers (``STATE``/``SCHEDULER``/
                              ``CLOCK``/``LOGGER``/``RECORDABLE_STATE``/
                              ``TS_OUT``/``TSB_OUT``) and the lazy type-kind
                              caches.
``_wiring/_operator.py``      ``@operator``, overload registration, the wire
                              trampoline, dispatch machinery.
``_wiring/_node.py``          ``_PyNode`` (signature binding + call
                              normalisation), ``@compute_node``/``@sink_node``,
                              ``lift``, ``@generator``, ``push_queue``.
``_wiring/_graph.py``         ``_GraphFn``/``@graph``, graph-fn wrapping,
                              auto-resolution, ``@component``.
``_wiring/_compose.py``       Higher-order wiring (``map_``/``reduce``/
                              ``switch_``/``mesh_``), ``combine``/``convert``/
                              ``collect``/``emit``, ``feedback``,
                              ``DebugContext``, casts.
``_wiring/_services.py``      Service/adaptor decorators and impl binding,
                              ``context``.
``_wiring/_runner.py``        ``run_graph``, ``evaluate_graph``, and the
                              ``eval_node`` test harness.
============================ ==================================================

The siblings are unchanged: ``_types.py`` (type expressions), ``_compat.py``
(upstream-parity shims + known-gap machinery), ``_signature.py``, ``_table.py``,
``nodes.py``, ``arrow.py``, and the thin ``test``/``stream``/``adaptors``
subpackages.

Two structural rules keep the package importable:

- The intra-``_wiring`` *top-level* import graph is acyclic
  (sentinels → state → markers → operator → node → graph → compose, with
  ``_core`` feeding everything); forward references are resolved by lazy
  in-function imports only.
- Cycles are broken by *lazy in-function imports* (there are ~55 of them, e.g.
  ``._types`` ↔ the wiring layer). When moving code, preserve the lazy edge —
  promoting one to module top is how import-order bugs are born.

Teardown and immortality
------------------------

The single most misleading-looking property of the bridge: **many things leak
on purpose.**

The core registries (TypeRegistry, plan factories, OperatorRegistry) and the
bridge-side registries (graph-callable records, the WiredFn context table,
node records, the enum/bundle class maps, the slot objects in
``include/hgraph/python/bridge_state.h``) are immortal ``new``-leaked
singletons. The reason is recorded history, not sloppiness: in a shared
module, cross-TU static destruction order destroyed interned bindings *before*
the operator impls' default ``Value``\ s that referenced them — a segfault at
interpreter exit. The rule that fell out: **registries outlive everything**;
long-lived immutable artifacts are never destroyed, and Python objects held by
immortal records deliberately survive interpreter teardown.

Consequences you should expect rather than fix:

- ``nb::set_leak_warnings(false)`` in module init is intentional — nanobind
  would otherwise report the immortal records as leaks at exit.
- Leak tools (ASAN leak checker, valgrind) will report the registries and the
  ``bridge_state.h`` slots. That is the design.
- ``reset_registries()`` clears and re-seeds (bumping
  ``_registry_generation()``, which keys Python-side caches like the
  compound-type cache in ``_types.py``); it is the only sanctioned way to
  "free" registry state, and metadata handles must be re-looked-up afterwards.

GIL boundaries
--------------

The runtime evaluates without the GIL: ``PyWiring::run`` releases it the
instant the native run loop is entered. The Python bridge configures the
executor's optional phase runner with one ordinary
``nanobind::gil_scoped_acquire``. That runner invokes the complete root start
phase, each complete root evaluation cycle, and the complete stop-and-cleanup
phase inside the guard. Evaluation notifications, lifecycle observers, nested
graphs, argument/result conversion, user calls, exception translation, and
Python-owned node teardown therefore share the phase's single acquisition.

The guard and phase invocation are nested on one C++ stack; no GIL ownership
state is handed between observers, callbacks, or nodes. An escaping exception
unwinds the evaluation guard normally, after which error cleanup enters the
stop phase under a fresh guard. A cycle containing any number of Python nodes
still performs exactly one acquire/release pair. The GIL is free during native
executor scheduling and while the real-time loop waits between cycles,
preserving the sender-liveness guarantee for Python feeder threads.

Runtime ``GlobalState`` access follows the normal injectable path. A Python
graph or node declares a ``GlobalState`` parameter; the bridge constructs its
guarded projection directly from the native ``GlobalStateView`` injected into
that callback. ``GlobalState.instance()`` is wiring/configuration convenience
only and raises during graph execution, preventing an ambient runtime-state
path from competing with injection. Nodes that do not request the injectable
perform no runtime-state lookup or wrapper construction. The fast compute path
does not accept injectable arguments and keeps its transient-view guard in its
node-owned cache, so evaluation uses neither C++ thread-local state nor a
process-global runtime context.

The phase runner is an optional, first-class C++ executor facility and is
unset for ordinary native C++ execution. Nodes that need an embedding context
declare ``requires_phase_runner``; Python nodes do so, and nested-node builders
propagate that property from their child plans. The Python bridge installs the
runner only when the finished root graph opts in (or when Python lifecycle
observers were supplied);
``lower`` applies the same rule. A pure-native graph authored through Python
therefore retains the native executor fast path, while dynamically-created
nested Python graphs remain covered without thread-local activation or
per-node fallbacks. Python work outside executor phases — wiring callbacks,
overload resolution, Python-owned values whose lifetime can escape a run, and
exceptional notification-callable destruction — retains a local guard.
Push-source senders still *release* the GIL around the blocking C++ send from
Python threads.

The lock-ordering rules live in :doc:`python_integration` (*GIL And Runtime
Locks*). Keep the phase guard around the complete executor phase; do not
extend it across native executor scheduling or waits.
The historical before/after record was consolidated into the final benchmark
cut. The current cross-platform performance record is
``benchmarks/results/baseline-summary-20260809.md``.

Consumer-selected Python value storage
--------------------------------------

Issue #204 avoids repeated Python-to-C++-to-Python conversion without a
run-global side table. During graph completion, wiring classifies every
ordinary ``TS`` output by its readers and asks ``ValuePlanFactory`` for one of
three physical representations:

* native-only outputs keep the canonical C++ value;
* outputs with both Python and native readers keep the canonical C++ value
  plus one inline, optional retained ``PyObject``;
* Python-produced outputs whose complete readership is Python retain the
  normalized Python value directly and do not construct the corresponding C++
  value.

The declared schema does not change. The value factory owns the representation
policy and may conservatively decline either Python-aware request. The first
implementation retains standard scalar strings/bytes, Python-owned named
Bundles, fixed tuples, and dynamic lists or variadic tuples whose elements are
recursively retainable. It declines Python-aware storage for cheap
bool/int/float values, mutable set/map storage, shaped arrays, ``Any``, and
other schemas until measurement and a complete read-shape policy justify them.
Existing Python-owned named-Bundle bindings already provide the direct
representation.

Python-only storage requires a complete proof: the producer is a Python node,
every direct reader is a Python node, and the output does not cross a graph or
nested-graph boundary. Native readers, child projections, forwarding
endpoints, record/replay boundaries, and escaped graph outputs force native
storage. A mixed output leaves its ordinary value binding canonical so typed
C++ ``ValueView`` checks remain valid; its retained object is an adjacent field
in the same planned allocation.

Writes validate and normalize the Python read shape before retaining the
object. For example, a list returned for ``TS[tuple[T, ...]]`` is retained as a
tuple. Mixed writes populate both fields, native writes invalidate the inline
cache, and a Python read lazily fills an empty cache after converting a native
value. The holder is destroyed with the output's normal planned storage, so
nested graph deletion, slot reuse, exceptions, and run teardown all follow the
existing output lifetime protocol; no TLS lookup, per-tick hash probe, or
node-stop cleanup hook is involved. Returning the same object is within
contract because output values are immutable by graph semantics (mutating one
from Python is UB) and matches upstream hgraph's reference-passing behaviour.

Python-owned Bundle bindings
----------------------------

RFC 0004 adds a second owning representation for named Bundles without adding
a value kind. ``src/hgraph/python/bridge_state.cpp`` owns the bridge-only
``PythonBundleValue`` and its binding registry. A value retains the exact
``PyObject *``, its active concrete schema, its realised binding, and a lazy
cache of projected fields. The public C++ surface remains
``ValueTypeRef``/``BundleView``; neither ``PyObject`` nor the carrier layout is
part of the SDK.

The binding is deliberately non-composite but supplies complete read-only
``IndexedValueOps``. ``element_at`` performs normal Python attribute lookup
under the GIL and converts only the requested field into its declared realised
binding. Generic Bundle code therefore selects on indexed capability, not on
Python ownership. Whole-object copy, move, conversion, equality, hashing,
formatting, and destruction acquire the GIL where Python is involved.

The named Bundle's anonymous structural twin remains the assembly format.
``TSB[PythonClass]`` stores that field-expanded shape, while
``BundleBuilder`` and ``as_scalar_ts`` erase the source and ask the owning
binding to construct the Python class. The optional
``ValueOps::can_materialize_source`` policy permits construction when every
required constructor field is valid, even if defaulted or ``init=False``
fields are absent. Bindings without that capability keep the normal
all-fields-valid rule.

Python-side schema extraction and generic specialisation live in
``hgraph._types``. Class identity, not rendered name or field shape, is the
registration key. A concrete generic alias is retained beside its shared
Python origin so ``Box[int]`` and ``Box[str]`` have distinct nominal schemas
and dispatcher tags. Runtime conversion first uses exact class/specialisation
information, then MRO and field-schema matching; ambiguous inference is an
error.

Registration records and owning bindings follow the bridge's immortality
rule. ``reset_registries`` clears the lookup maps and invalidates Python-side
generation-keyed caches, but already-published binding objects are never
destroyed during interpreter teardown.

Serialized wiring, concurrent execution
---------------------------------------

Wiring state is a module-level stack (``_wiring/_core.py::_wiring_stack``),
*not* a thread-local. Top-level ``run_graph`` / ``evaluate_graph`` authoring is
serialized by ``_wiring_lock`` because both that stack and native operator
registration are process-wide. The lock remains held through
``Wiring::finish()`` and service materialization, where C++ can re-enter Python
wiring, and is released as soon as the native executor has been constructed.
The executor run therefore does not hold the wiring lock: distinct native
executors can progress concurrently on different threads.

C++ re-enters Python wiring through the *borrowed wiring* pattern: when the C++
side calls back into a Python graph function (graph-fn wrapper) or a Python
overload (wire trampoline), it hands over a borrowed ``PyWiring``; the Python
side pushes it onto ``_wiring_stack``, wires, and pops. Both re-entry sites
follow the same push/try/finally/pop shape — if you add a third, copy it exactly.

``_wiring_stack`` must remain the **same list object** everywhere it is
visible (``hgraph._wiring._core``, the ``hgraph._wiring`` aggregation, and
``arrow.py``'s import); nothing may ever rebind it.

Private submodules (``hgraph._wiring``, ``hgraph._types``, ``hgraph._compat``,
…) are **internal-only**: test code and anything outside the package import
from the ``hgraph`` root (which re-exports the full supported surface) or the
public subpackages. Test code that needs to intercept wiring uses the
sanctioned seam ``hgraph.test.use_wiring(stub)`` rather than touching the
stack. (The one recorded exception: the compound-scalar hierarchy tests read
the interned value-type metadata via ``hgraph._types._value_type`` — there is
no public metadata-introspection surface, and adding one sits inside the
shelved CompoundScalar design space.)

The erased operator contract
----------------------------

Registry pattern-matching owns **all** dispatch. The bridge never selects an
overload by parameter *label*, operator *name* heuristics, or Python-side
type tests — that lesson recurred three times before it became a standing
ruling (see :doc:`operators` and the parity matrix). Calling-convention
questions (does a subscript name the output? does a scalar kwarg lift to
``const``?) are answered by registry introspection
(``operator_output_is_selective``, resolution retries), never by name.

Python-defined operators register under ``__pyop__{qualname}_{id:x}`` — the
id-suffix exists because the C++ operator registry is process-global and
Python may define two distinct operators with the same qualname (REPL,
parametrised test fixtures). Do not "clean up" the id.

The ``hgraph`` package exposes every registered operator as a module-level
attribute via PEP 562 (``__getattr__`` in ``__init__.py`` resolving through
``operator_function``): operator attributes *appear on first access* and are
then cached in module globals. ``dir(hgraph)`` before first access therefore
understates the surface; that is lazy resolution, not a missing export.

Value and reference crossings
-----------------------------

- **References are values** (ruling 2026-07-05): ``REF`` crosses the boundary
  as an opaque value; ``.output`` is deliberately not exposed. A non-REF
  parameter bound to a REF source receives the *dereferenced* value (binding
  inserts the from-REF adaptation); a REF parameter receives the reference
  itself — ``bundle_port``'s reference-shape handling implements this and
  carries the ruling comment.
- **Set deltas are shaped by class identity**: a plain ``frozenset`` crossing
  into a TSS applies as the *full value*; a ``_SetDelta`` (registered with C++
  at import via ``_set_set_delta_class``) applies as a *delta*. Same duality
  for ``REMOVED``/``_Removed`` on TSD keys. This is why those classes must be
  defined exactly once and only re-exported.
- **Unwired optional ts inputs** cross as ``None`` (a never-ticking null
  source is wired under the hood) — a Python node seeing ``None`` for an
  unwired input is contract, not a lost value.
- **No kind-switches in conversion**: Value ↔ Python conversion binds onto the
  per-type *ops tables* (``python_conversion_traits`` hooks), never a switch
  over value kinds in the bridge (ruling 2026-07-07). If a new value kind
  needs conversion, extend its ops, not ``value_conversion.cpp``.

Per-tick application is registry-free (ruling 2026-08-15)
---------------------------------------------------------

Type resolution is what wiring is for. When a Python node's result is applied
to a time-series output (``apply_python_result`` →
``PythonTSDataOps.apply_result_impl``), the per-kind implementation consumes
the Python object in a **single fused pass** against bindings that wiring
already resolved onto the TSData layout (``TSDataLayout.value_binding`` /
``delta_binding``, ``TSSDataLayout.key_binding``, the TSD layout's key and
element bindings), recursing through child kinds' ``apply_result_impl``. It
must not build an intermediate delta ``Value``, must not call the
schema-directed ``py_to_value_as`` hook, and must not consult
``TypeRegistry`` / ``ValuePlanFactory`` / ``TypeRealizationSnapshot`` — those
are wiring-time machinery behind mutexes (the 2026-07-02 lock-free ruling).
The 0.8.15 regression (2–3x on every TSD workload) was exactly this flaw:
routing the per-tick apply through the generic delta pipeline re-resolved
type bindings per key per tick under the registry mutexes.

``delta_from_python`` (the materialised delta-``Value`` pipeline) still
exists for consumers that need a delta *object* — record/replay ingest,
current-state transfer — and remains builder-based; it is not the per-tick
apply path.

**Enforcement**: every type-system mutex is a ``TypeSystemMutex``
(``types/utils/counted_mutex.h``), counted in ``type_system_lock_count()``
and surfaced as ``RuntimeRegistrySnapshot.type_system_lock_acquisitions``.
This includes the codec converter-interning mutexes (``json_codec.cpp``,
``table_codec.cpp``) — converter synthesis is type-system machinery, and an
uncounted mutex there hid the JSON operators' per-tick converter lookups from
the counter until the 2026-08-15 std-operator audit.
``python/tests/test_registry_snapshot.py`` asserts that warm runs of the same
generator-driven graph at different cycle counts acquire identical lock
counts, and extends the same N-vs-2N invariant across an **operator-family
lock matrix** (``test_operator_family_lock_matrix``): one graph per operator
group the audit found (or cleared of) acquiring locks per tick. Families that
still violate the ruling are marked ``xfail(strict=True)``, so fixing an
operator forces the marker's removal in the same change; passing families are
permanent regression guards. Known, deliberate exceptions the test does not
cover:

- ``eval_node``'s own harness (input injection and per-tick output recording)
  converts through delta values each tick — test tooling, not the production
  path.
- Keyed-collection *lifecycle* events (a ``map_`` child graph created or
  destroyed on key churn) run build machinery; the cost is per key event, not
  per cycle.
- ``apply_ref_result`` still converts through ``py_to_value_as`` when a
  Python node writes a ``REF`` value directly (rare; not observed on any
  benchmarked path).

Two sanctioned cache patterns keep hot paths off the mutexes without
weakening reset semantics (the registry's ``reset()`` is test-only but
frees interned records): **start-resolved plans** — a node resolves every
binding/converter its shape needs in its ``start`` hook and carries them in
node ``State`` (``ResolvedBindings`` in ``lib/std/value_util.h``, the JSON
operators' ``TsJsonPlan``); and **generation-checked thread-local caches**
for process-wide helpers (``TypeRegistry::scalar_type<T>()`` behind
``Value{T}``, the JSON ``json_meta``/``json_value_binding`` accessors),
validated against the lock-free ``TypeRegistry::reset_generation()``
counter — the same invalidation discipline as the table codec's layout
cache.

Platform notes
--------------

- **Run log capture**: Python graph runs install a native spdlog sink that
  forwards to ``GraphConfiguration.graph_logger``. Use ``caplog`` for mixed
  native/Python graph logs. ``_hgraph.reset_logger()`` remains only for tests
  that exercise the process-default C++ logger directly.
- **Windows DLLs**: there is no rpath on Windows; the build copies Arrow (and
  pyarrow-support) DLLs beside the extension so ``import _hgraph`` works
  before ``pyarrow`` is imported (``python/CMakeLists.txt``).
- **Editable installs cache the native build**: after C++ edits, rebuild with
  ``uv pip install -e . --reinstall`` — a plain ``-e .`` silently reuses the
  cached extension, and the symptom is Python tests failing against *old*
  native behaviour.

When it looks wrong but isn't
-----------------------------

===================================================== =========================================================
Symptom                                               Intentional cause
===================================================== =========================================================
Leak reports for registries / records at exit         Immortality rule: registries outlive everything.
``nb::set_leak_warnings(false)``                      Same — silences the intentional immortal records.
Operator missing from ``dir(hgraph)``                 PEP 562 lazy surface; it appears on first access.
Missing Python graph ``log_`` output                  Capture/configure ``GraphConfiguration.graph_logger``.
Python node gets ``None`` for an input                Unwired optional input: the null-source contract.
``frozenset`` set-delta replaced the whole TSS        Full-value vs ``_SetDelta`` class-identity shaping.
Ugly ``__pyop__…_1f3a`` registry names                Deliberate: process-global registry, id disambiguates.
Two identical register_overload lists (historical)    Now single ``register_python_overloads()`` — keep it so.
Python tests fail right after C++ edits               Stale editable install; ``uv pip install -e . --reinstall``.
No ``hgraph._runtime`` module                         Split into ``hgraph._wiring/`` (2026-07); import from there.
===================================================== =========================================================
