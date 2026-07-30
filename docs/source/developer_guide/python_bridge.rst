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
                              list), ``WiringPort`` + all attached sugar,
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
instant the run loop is entered. Within a run, the hold is **cycle-scoped
and coarse-grained** (Howard's ruling 2026-07-30, ``python/py_cycle_gil.h``):
once the run is known to contain python nodes, ``PyCycleGilObserver`` takes
the GIL at the start of every root evaluation cycle and releases it when the
*root* graph's after-evaluation notification fires (normal completion and
escaping exceptions alike). Every python re-entry keeps its ordinary local
acquire — under the held cycle lock that is a cheap recursive ensure, and
there is no hold hand-off between trampolines and observer to get wrong.
The GIL is therefore always free while the real-time loop waits between
cycles, preserving the sender-liveness guarantee; what changed is only that
N per-node acquire/release pairs inside one cycle collapsed to one (measured
at ~6% of ``pthread_mutex`` traffic on dense python graphs).

"Contains python nodes" is detected at runtime: the node START trampolines
call ``py_cycle_gil_note_python_call()``, which registers the observer on
first use — *last* on the executor's lifecycle-observer list, so every other
observer's after-hook still sees the GIL held. Pure-native runs never
register it and pay nothing. The note reads a ``thread_local`` holder
(matching the per-thread active-runtime guard, so concurrent per-thread runs
each arm only their own observer); that read happens once per node *start*,
never on the per-tick path — general-dynamic TLS through the bridge ``.so``
is the expensive access pattern and is deliberately kept off the tick path.
Two safety nets bound a buggy observer that throws out of the
after-notification before the release runs: the next root
before-notification finds the hold still armed and keeps it (no
double-ensure), and the run wrapper releases on exit, so the ``PyGILState``
pairing always closes. The startless diagnostic nodes (``type_py``,
``getattr_type_name``), one-time start/stop bodies, the overload wire
trampolines and ``requires`` bridges, ``io_write_slot``, the lowered
``run_lowered`` frame path (no observer is armed there), and push-source
senders (which *release* around the blocking C++ send from Python threads)
all keep their plain local acquires. The lock-ordering rules live in
:doc:`python_integration` (*GIL And Runtime Locks*); the implementation rule
stays: **GIL scopes move verbatim** — when relocating code, never widen or
narrow an acquire/release, and keep the ruling comments attached to
``PyWiring::run``, ``py_nodes.cpp``, and the sender.

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

The single-threaded wiring model
--------------------------------

Wiring state is a module-level stack (``_wiring/_core.py::_wiring_stack``),
*not* a thread-local — the runtime is single-threaded by design (ruling
2026-07-02; the standing "no thread_locals" rule). C++ re-enters Python wiring
through the *borrowed wiring* pattern: when the C++ side calls back into a
Python graph function (graph-fn wrapper) or a Python overload (wire
trampoline), it hands over a borrowed ``PyWiring``; the Python side pushes it
onto ``_wiring_stack``, wires, and pops. Both re-entry sites follow the same
push/try/finally/pop shape — if you add a third, copy it exactly.

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
