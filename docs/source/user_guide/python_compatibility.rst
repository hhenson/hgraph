Python compatibility
====================

The ``hg_cpp`` distribution provides the public ``hgraph`` Python package on
top of the C++ runtime. Install it in an isolated environment: the upstream
``hgraph`` distribution exports the same import package and the two
distributions must not be installed together.

API boundary
------------

The documented ``hgraph`` package, its public adaptors, and Python node/graph
decorators are the supported Python API. ``_hgraph`` and modules or attributes
whose names start with an underscore are implementation details. The Python
API is the compatibility commitment for the ``0.4`` release line. The native
C++ authoring API remains source- and binary-provisional while it is refined
by production use.

Python-authored compute, sink, generator, graph, service, adaptor, component,
and push-source code executes in the C++ runtime. Python adapts values and
callables; it does not implement a second graph engine.

``set_pooled_compound_scalar_storage()`` is an optional graph-level memory
policy for wide polymorphic ``CompoundScalar`` hierarchies.  Call it inside the
``GlobalContext`` which wires the graph.  It changes only the native graph
representation; the Python value API and graph behaviour remain unchanged.
The default inline policy adds no pool storage to the graph layout.

Runtime time-series views expose the authoring and interrogation surface backed
by the native endpoints. Input and mutable ``_output`` views provide
``owning_node``, ``owning_graph``, and ``is_reference()`` in addition to their
value, validity, delta, and modification properties. Input views also provide
``active``, ``make_active()``, and ``make_passive()``; mutable outputs provide
``all_valid`` and ``last_modified_time``.

The old Python engine's endpoint-topology object model is intentionally not
part of this API. Python views do not expose ``bind_output()``,
``un_bind_output()``, ``do_bind_output()``, ``do_un_bind_output()``, or
``re_parent()``. Direct bound-output and parent-input/output traversal,
peer/bound topology inspection, and output subscription hooks are likewise
native runtime responsibilities. Graph authors express topology through wiring
and ``REF`` values instead of mutating live endpoint ownership from Python.

Upstream's ``hgraph.nodes`` wildcard also exposes several private transport
helpers: ``capture_output_node_to_global_state``,
``capture_output_to_global_state``, ``get_shared_reference_output``,
``mesh_subscribe_node``, ``request_id``, ``write_service_replies``,
``write_service_request``, and ``write_subscription_key``. They depend on the
old Python owning-node and ``GlobalState`` mutation protocol and are
intentionally not exposed by ``hg_cpp``. Use the public service, adaptor, and
``mesh_`` / ``get_mesh`` APIs; these execute through the native runtime in both
C++ and Python.

Run diagnostics
---------------

``GraphConfiguration(trace=True)`` installs the native evaluation tracer.
``profile=True`` installs the native aggregate profiler; a dictionary may set
``start``, ``eval``, ``stop``, ``node``, ``graph``, and ``recent_window``.
Pass an explicit ``hgraph.test.EvaluationProfiler`` when code needs the owned
snapshot after the run:

.. code-block:: python

   from hgraph import GraphConfiguration, evaluate_graph
   from hgraph.test import EvaluationProfiler

   profiler = EvaluationProfiler(recent_window=50)
   evaluate_graph(app, GraphConfiguration(profile=profiler))
   snapshot = profiler.snapshot()

The snapshot reports graph cycles, wall/evaluation time, real-time scheduling
lag, runtime load, and per-path start/evaluation/stop aggregates. Native
``log_`` nodes, Python ``LOGGER`` injectables, trace output, and runner messages
all use ``graph_logger`` for that run. ``default_log_level`` and
``logger_formatter`` therefore apply consistently to mixed graphs.

Custom observers may subclass ``EvaluationLifeCycleObserver`` and pass an
instance through ``GraphConfiguration(life_cycle_observers=(observer,))`` or
``eval_node(..., __observers__=[observer])``. Callback arguments are guarded
views over native runtime objects: inspect them inside the callback and retain
ordinary values such as ``graph_id`` or ``label``, not the view itself.

Wiring diagnostics are separate from runtime lifecycle diagnostics.
``GraphConfiguration(trace_wiring=True)`` prints the native wiring trace; a
dictionary accepts ``filter``, ``graph``, and ``node`` options. ``eval_node``
uses the same path through ``__trace_wiring__``. The native
``hgraph.test.WiringTracer`` may be supplied through ``wiring_observers`` when
its collected ``lines`` are needed programmatically:

.. code-block:: python

   from hgraph import GraphConfiguration, evaluate_graph
   from hgraph.test import WiringTracer

   tracer = WiringTracer(filter="orders")
   evaluate_graph(app, GraphConfiguration(wiring_observers=(tracer,)))
   print("\n".join(tracer.lines))

Wiring context and lifecycle (``WiringGraphContext`` and friends)
------------------------------------------------------------------

Upstream spreads wiring-time bookkeeping across ``WiringGraphContext``,
``WiringContext`` and ``WiringNodeInstanceContext``. Here the native
``Wiring`` owns all of it; the python ``WiringGraphContext`` is a deliberate
five-method facade (``instance``, ``build_services``,
``add_service_build_context``, ``registered_service_clients``,
``built_services``) — the surface the tornado/perspective server-adaptor
pattern actually uses (theme-B ruling 2026-08-01). Porting map for the rest:

- ``add_sink_node`` / ``reassign_items`` — the concept is replaced: sink
  nodes are ordinary interned wiring instances, and reparenting/ordering is
  expressed with the native ranking primitives (``add_rank_dependency``,
  ``add_same_cycle_pair``).
- ``wiring_path_name`` / ``label_nodes`` — diagnostic paths are computed
  live from ``Wiring.current_wiring_path()``; label scopes come from the
  wiring-scope machinery (``__label__`` on ``map_``, graph labels).
- ``register_service_client`` (as a context method) — service registration
  is native (``Wiring::register_service_client_path``/``_rank``) behind the
  ordinary service stubs; read back with ``registered_service_clients()``.
- ``__stack__`` / frame mechanics — a single module-level wiring stack over
  the active native ``Wiring``.

The runtime ``Graph``/``Node`` **mutating** lifecycle
(``start``/``stop``/``dispose``/``initialise``/``schedule_node``/
``evaluate_graph``; ``Graph.copy_with`` is dead code upstream) is
internal-only in both engines — user-facing control goes through the
``SCHEDULER`` injectable, ``EvaluationEngineApi`` (notifications,
``request_engine_stop``), and the run entry points. The read-only
introspection surface (``graph_id``, ``node_id``, ``nodes``,
``parent_node``, ``signature``, ``schedule``…) that trace/profiler/inspector
build on is fully present.

Test-package recording (``record_to_memory`` and friends)
----------------------------------------------------------

Upstream's in-memory test recording (``record_to_memory``,
``replay_from_memory``, ``set_replay_values``, ``SimpleArrayReplaySource``)
was prototyped in the test package and later superseded by the formal
record/replay machinery. hg_cpp deliberately does not replicate the legacy
spellings (ruling 2026-07-31); port test code onto the formal API:

- record: ``set_record_replay_config`` / ``record_replay_scope`` with the
  in-memory model, and read results back through ``get_recorded_value`` or
  ``Run.recorded(key)``;
- replay: seed inputs with ``Wiring.set_replay(key, values, ts_type=...)``
  before running (the same mechanism ``eval_node`` uses — per-cycle vectors
  with ``None`` for no-tick), replacing
  ``set_replay_values``/``SimpleArrayReplaySource``; read results back with
  ``Run.recorded(key)`` or ``get_recorded_value``;
- for simple per-cycle vectors, ``hgraph.test.eval_node`` already provides
  the seed-and-capture loop directly.

Python-authored wiring observers are deliberately unsupported. The observer
interface and its event records are C++ diagnostics APIs; Python configuration
currently accepts only the bound native ``WiringTracer``.

The runtime inspector follows the same native-observer rule. Register an
``Inspector`` before execution and retain that handle to read an owned snapshot
after the graph has stopped:

.. code-block:: python

   from hgraph import GraphConfiguration, evaluate_graph
   from hgraph.debug import Inspector, inspection_rows

   runtime_inspector = Inspector(recent_window=50)
   evaluate_graph(
       app,
       GraphConfiguration(life_cycle_observers=(runtime_inspector,)),
   )
   rows = inspection_rows(runtime_inspector.snapshot())

The rows contain graph/node hierarchy, schemas, schedules, evaluation counts
and timings, and current/peak static and nested-slot storage. This replaces the
upstream Python runtime-object walker. It is safe for pure native, mixed, and
keyed nested graphs; an optional table, notebook, or HTTP UI is a presentation
consumer of these rows rather than part of runtime inspection.
``Inspector.reset()`` raises ``RuntimeError`` while an executor is active and
otherwise clears the owned history.

``trace_back_depth`` bounds the native activation trace attached to an
uncaught run error; ``capture_values=True`` includes current input values.
``cleanup_on_error=False`` defers node stop while the raised exception remains
alive. Once the exception is released, executor destruction performs the
mandatory final teardown.

Runtime callables
-----------------

``apply(fn, *args, **kwargs)`` invokes a callable value at evaluation time and
publishes its result. ``call(fn, *args, **kwargs)`` invokes it for side effects.
Both operators use the native ``ValueCallable`` scalar and packed C++ runtime
nodes. A plain Python callable is adapted to the same value type at the bridge.

All positional inputs must be valid before invocation. Invalid keyword inputs
are omitted, allowing the callable's Python default to apply. A plain callable
with a return annotation lets ``apply`` infer its output type; otherwise use
``apply[TS[Result]](...)``. C++ authors use ``value_fn<F>()`` for a native
runtime callable.

Scalar boundaries
-----------------

``TS[object]`` is the native type-erased ``Any`` schema. Values with a native
hgraph representation retain their concrete type record, so native conversion
and ``type_`` dispatch remain available. An arbitrary Python object is retained
as an opaque bridge value only when no native representation exists.

Python-owned structured scalars
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A standard-library dataclass may be used directly as a nominal scalar schema:

.. code-block:: python

   from dataclasses import dataclass
   from hgraph import TS, combine, graph

   @dataclass(frozen=True)
   class Quote:
       instrument: str
       bid: float
       ask: float = 0.0

   @graph
   def spread(quote: TS[Quote]) -> TS[float]:
       return quote.ask - quote.bid

   @graph
   def make_quote(bid: TS[float]) -> TS[Quote]:
       return combine[TS[Quote]](instrument="ABC", bid=bid)

The value held by ``TS[Quote]`` is the exact Python object that was emitted.
Its ordered annotations provide a nominal Bundle schema for wiring,
reflection, field access, generic resolution, and dispatch. Attribute
projection is lazy: assigning the whole object does not recursively validate
its fields, while reading a declared field converts that attribute to the
declared output type and reports an error if it is incompatible.

``TSB[Quote]`` is the canonical field-expanded time-series form; callers do
not need to publish a peer ``TimeSeriesSchema`` created with
``TimeSeriesSchema.from_scalar_schema``. ``as_scalar_ts()`` constructs
``Quote`` using keyword arguments and honours dataclass defaults, default
factories, keyword-only fields, ``init=False``, and ``__post_init__``.
Generic dataclasses such as ``Box[int]`` retain their concrete specialisation
through storage, Bundle conversion, and runtime dispatch.

The lift is deliberately conservative. Every stored dataclass field ``field:
T`` becomes ``field: TS[T]``. Scalar collections remain scalar values, and a
nested dataclass remains ``TS[Nested]`` rather than becoming a nested ``TSB``;
the runtime does not infer ``TSL``, ``TSD``, ``TSS``, or another time-series
topology from a scalar annotation. ``ClassVar``, ``InitVar``, and computed
properties are not stored fields. Unresolved annotations and time-series
annotations inside a scalar dataclass are rejected while constructing the
schema.

An annotated non-dataclass class must opt in:

.. code-block:: python

   from hgraph import register_python_object_type

   @register_python_object_type
   class LegacyQuote:
       instrument: str
       price: float

       def __init__(self, price: float, instrument: str = "ABC"):
           self.instrument = instrument
           self.price = price

``register_python_object_type`` also accepts an ordered ``fields`` mapping.
Constructor parameters used to reconstruct a value must be keyword-capable;
required constructor parameters not represented in the schema are rejected.

Use native ``CompoundScalar`` for records that need native field layout,
Python-free execution, or repeated low-cost field access in C++. Use a plain
dataclass or registered class when preserving Python constructors,
descriptors, identity, permissive values, or custom equality is the priority.
Both forms are Bundles and therefore share the storage-independent
``BundleView`` and graph-operator contract.

Python-owned values have snapshot-by-emission semantics. Mutating an object in
place does not create a tick; emit a new object to publish a change. Equality
and hashing follow the class's Python methods. An unhashable class can be a
``TS`` value but is rejected as a ``TSS`` or ``TSD`` key.

Native ``DateTime`` is a timezone-naive UTC value. Python timezone-aware
``datetime`` values are normalized to UTC and then made naive on ingress;
naive values are already interpreted as UTC. Timezone-aware standalone
``time`` values are rejected because they cannot be normalized without a date.

Data frames
-----------

``Frame`` values use Arrow storage. The typed ``from_data_frame[OUT]`` operator
honours ``OUT`` exactly and rejects Arrow columns whose type does not match the
requested scalar schema. It never rewrites an explicit output type from the
data. The data-source convenience functions infer unresolved key/value types
from the Arrow schema before wiring, then call the same native operator with a
concrete output type. They pass the executor's configured start and end times
to ``DataFrameSource.iter_frames`` and consume provider batches once, without
eagerly combining the complete source.

``PolarsDataFrameSource`` keeps the upstream constructor and converts its
input at the Python boundary. ``ArrowDataFrameSource`` is an additive
hg_cpp extension for callers that already hold a PyArrow table; both feed the
same Arrow-native C++ path.

Intentional differences from upstream hgraph are maintained in
:doc:`../developer_guide/roadmap` under *Accepted Deviations*. They are part of
the compatibility contract rather than temporary Python-side workarounds.
