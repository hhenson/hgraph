Python Integration
==================

Python integration exists to preserve the current ecosystem and to support Python user-authored nodes. It should not define the core runtime architecture.

This page records the behavioural contract of the bridge. For the
*implementation* layout and the non-obvious invariants (immortal registries,
GIL boundaries, the wiring stack, the facade structure), see
:doc:`python_bridge`.

Supported Roles
---------------

- Python graph wiring,
- compatibility with existing Python HGraph user code,
- Python user nodes executed by the C++ runtime,
- packaging of optional bindings.

Boundaries
----------

Normal CMake builds should not require Python. Python-specific code should live behind optional CMake targets and be enabled only through:

.. code-block:: bash

   -DHGRAPH_BUILD_PYTHON_BINDINGS=ON
   -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON

GIL And Runtime Locks
---------------------

The C++ runtime must assume that it does **not** hold the Python GIL unless a
local scope has explicitly acquired it. Any path that calls Python code or uses
Python C API objects must acquire the GIL at that boundary. This includes Python
node ``start`` / ``eval`` / ``stop`` callbacks, lifecycle observers implemented
in Python, Python notification callbacks, Python-backed sender functions, and
exception translation that inspects Python exception state.

Conversely, the real-time engine must not hold the GIL while waiting on runtime
condition variables or other blocking primitives. It also must not hold graph,
node, sender, receiver, or clock mutexes while entering Python. The ordering
rule is:

1. release/acquire runtime locks only for C++ state,
2. drop those locks before calling Python,
3. acquire the GIL immediately around the Python call,
4. release the GIL before a blocking wait.

Rule 3 is satisfied by one local nanobind RAII guard around each complete
executor start, root evaluation-cycle, or stop phase. All Python nodes and
callbacks in that phase share the guard; it leaves scope before the executor
resumes scheduling or can block. See :doc:`python_bridge`, *GIL boundaries*.

This is especially important for push-source nodes: external threads enqueue
through a sender and wake the real-time evaluation clock, while the evaluator may
be sleeping on a condition variable. The implementation must avoid GIL/runtime
lock inversion in both directions.

Topics To Specify
-----------------

- GIL ownership during node evaluation,
- Python object lifetime inside C++ runtime state,
- exception translation,
- Python callback scheduling,
- conversion between Python type metadata and C++ schemas,
- packaging and ABI policy.


Cross-boundary type identity
----------------------------

The C++ runtime identifies schemas by **pointer equality of interned metadata**:
two equivalent schemas resolve to the same ``const TSValueTypeMetaData*`` /
``const ValueTypeMetaData*`` because the ``TypeRegistry`` interns them. Anything
that matches types across the boundary — notably *operator* overload dispatch
(see *Operators*) — relies on this: a Python ``TS[int]`` and a C++ ``TS<Int>``
must produce the **same** interned pointer.

The invariant that guarantees it: there is exactly **one canonical scalar per
logical type**, and every name a Python type uses is an **alias** onto that
canonical scalar (via ``register_value_alias``), never a separately-interned
synthetic. Concretely, ``value_type("int")`` (the Python lookup) must return the
*same* pointer as ``register_scalar<int>("int")`` (the C++ registration); since
``TypeRegistry::ts(value_meta)`` interns on the value pointer, identity then
composes upward automatically (``TS``, ``TSL``, ``TSD``, …). The standard-types
seed must run **before** any overload is registered on either path; the C++ test
listener seeds via ``register_standard_types()`` and the Python module (when it
is built) must seed at import via a ``register_builtin_value_types()`` entry
point — the two must agree on names and aliases.

Downstream native scalar classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A downstream nanobind module associates an extension-owned C++ scalar with its
Python class through the installed
``hgraph/python/native_scalar_registration.h`` header:

.. code-block:: cpp

   auto cls = nanobind::class_<Price>(module, "Price");
   hgraph::python_bridge::register_native_scalar_type<Price>(
       cls, "extension.price");

The helper registers the scalar in the shared ``TypeRegistry`` and installs a
process-wide, bidirectional class/schema association. The same pair may be
registered repeatedly; a class or schema already paired with a different
counterpart is rejected. The registry retains the Python class, so
``TS[Price]`` resolves to the native schema and schema reflection returns the
same class.

The equivalent public Python operation is
``register_native_scalar_type(PythonType, native_value_type)`` from
``hgraph``. ``native_value_type`` may be a registered schema name or native
``ValueType`` handle. Extensions should not import or mutate
``hgraph._types``.

Hosting a Python node
---------------------

A Python user node is hosted without a new node family, role, or pointer type.
The bridge defines stateless C++ compute, sink, and generator trampolines and
builds them through the normal static-node front end. Their canonical
Node/Runtime ``TypeRecord`` selects the same schema, storage plan, ``NodeOps``
ABI, ``NodeTypeRef``, and ``NodePtr`` machinery as every other runtime node.
Only the record's representation label differs
(``hgraph.python.compute``, ``hgraph.python.sink``, or
``hgraph.python.generator``). The callbacks acquire the GIL, project the live
``NodeView`` into guarded Python-facing views, and write results through the
normal ``TSOutputView``. This is the mechanism behind a Python *operator*
implementation registering as an ordinary candidate (see *Operators > The
Python implementation path*), built only under
``HGRAPH_ENABLE_PYTHON_USER_NODES``.

The Bridge (Slice 1 — Landed)
-----------------------------

``bindings/python/`` (opt-in: ``-DHGRAPH_BUILD_PYTHON_BINDINGS=ON``; the
default build never needs Python) holds the nanobind module ``_hgraph``.
Slice 1 proves the contract end to end from Python:

- **Wiring by name**: ``Wiring.wire(name, args, kwargs, output_type=None)``
  builds erased ``WiringArg`` lists (ports and scalars, positional and
  keyword) and goes through the exact three calls the template-free proof
  established (``OperatorRegistry::resolve`` → ``impl->wire``); ports come
  back as opaque handles. ``ts_type("TS[int]")`` resolves expected-output
  schemas by registry name.
- **Running**: ``Wiring.run()`` finishes the wiring, builds a simulation
  executor and runs it; ``Wiring.set_replay(key, [values|None])`` seeds the
  replay buffer at wiring, ``Run.recorded(key)`` reads recordings back as
  Python values — the eval_node harness shape, from Python.
- **Eager const**: ``evaluate_const(name, args, kwargs, output_type)``
  exposes the P1 kernel.
- **Scalar conversion** (slice 1): bool/int/float/str + datetime/timedelta
  via the vendored nanobind chrono casters. Containers, bundles, Frame and
  the remaining atoms are the next conversion slice.
- Registration happens at module import; ``reset_registries()`` re-seeds.
  Type metadata handles must be re-looked-up after a reset.

Landed with a load-bearing fix: the core registries (TypeRegistry,
ValuePlanFactory, TSDataPlanFactory, OperatorRegistry) are **immortal**
(leaked) singletons — in a shared module, cross-TU static destruction
order destroyed interned bindings before the operator impls' default
``Value``\ s that reference them (a segfault at interpreter exit).
Registries hold process-lifetime immutable artifacts; they are never
destroyed. Tests: ``bindings/python/tests/test_bridge.py`` (registered
with ctest under the option).

The hgraph Package (Slices 2-4 — Landed)
----------------------------------------

``python/hgraph`` is the package that will eventually be **the** hgraph
package (Howard's direction); it mirrors the Python hgraph surface over
the ``_hgraph`` bridge module (built from ``python/module.cpp``):

- **Types**: ``TS[int]``, ``TSS[str]``, ``TSD[str, TS[int]]``,
  ``TSL[TS[int], Size[N]]``, ``TSB[SchemaClass]`` (``TimeSeriesSchema``
  annotations) — each subscription resolves to an interned C++ type handle.
- **Operator surface**: every registered operator (113) is a module-level
  function via PEP 562; ``WiringPort`` carries hgraph's dunder sugar
  (arithmetic/comparison/bitwise/unary, ``[]``, ``.field`` via
  ``getattr_``); ``const`` takes hgraph's ``tp=``. Two calling-convention
  rules are REGISTRY-driven, never name tests: a bare subscript type
  (``op[tp]``) names the requested OUTPUT when the operator's candidates
  can be selected by it (``operator_output_is_selective``) and otherwise
  types the INPUT series (``to_json[tp]`` — every overload shares one
  fixed output); a positional TYPE EXPRESSION argument
  (``const(value, tp)`` / ``nothing(tp)``) always names the requested
  output — the registry has no type-valued scalars, so a type in argument
  position is a wiring directive, whatever the operator.
- **Composition/evaluation**: ``@graph`` (nested graphs inline by calling),
  ``run_graph(fn, *args, start_time=, end_time=)`` returning
  ``[(time, value), ...]``, ``eval_node(fn, *vectors, __start_time__=,
  ``__end_time__=)`` with schema-directed test vectors (TSS from sets —
  removals via ``set_delta(...)``/``Removed(...)`` markers; a dict is NEVER
  a TSS value/delta and rejects loudly, except the empty ``{}`` which is
  upstream's empty-set stand-in — TSD from ``{key: value}`` dicts where a
  ``None`` value means NOTHING ticked for that key (the per-key analogue
  of the top-level no-tick, consistent across keyed structures — TSD
  keys, TSL indices, TSB fields — and matching upstream; removals are the
  explicit ``REMOVE``/``REMOVE_IF_EXISTS`` sentinels; ruling 2026-07-28),
  TSL from per-index lists) and friendly delta
  read-back (``REMOVE`` sentinel). A generic ``TSL[..., SIZE]`` parameter
  annotation resolves its size from the samples (int-keyed dict → max key
  + 1; list → length) and wires the REAL dynamic list shape (issue #81);
  only annotations the size binding cannot fully resolve fall back to
  sample-scalar inference. No implicit run bound is injected — a
  test that cannot quiesce sets ``__end_time__`` explicitly and says why.
- **Higher-order**: ``map_``/``reduce``/``switch_`` over **named operator
  callables** — the bridge pre-instantiates ``fn<X>()`` erasures for the
  stdlib markers (``wired_op``); ``switch_`` builds the ``SwitchCases``
  scalar; ``feedback(tp, initial)`` replicates the C++ feedback wiring
  erased (same node tags → same interning). ``delayed_binding(tp_or_port)``
  is the Python facade over the C++ wiring-only placeholder: it resolves before
  ranking and cannot create a runtime feedback edge. Associative ``reduce`` preserves
  omitted zero as a distinct arity: empty is invalid and a singleton bypasses
  the combiner; a supplied zero handles empty/singleton only and is ignored for
  two or more live values.
- **Value conversion**: all atoms (incl. date/time/bytes) + recursive
  containers both ways; ``evaluate_const`` exposes the P1 kernel.

Python Authoring Compatibility Contract
---------------------------------------

Python remains a supported authoring language over the C++ runtime.  The bridge
must keep these common workflows interoperable with native operators and graph
components:

- ``@graph`` composition with positional/keyword scalars, nested Python graphs,
  C++ operators, and Python runtime nodes in either direction;
- ``@compute_node`` and ``@sink_node`` with any practical arity, positional or
  keyword binding, scalar defaults, validity and activity gating, optional
  inputs, collection deltas, ``STATE``/``CLOCK``/``SCHEDULER``/``GlobalState``
  injectables, and Python ``start``/``stop`` lifecycle callbacks. Lifecycle
  parameters match the **eval signature by name and nothing else** (ruling
  2026-07-27, issue #79): a name eval declares takes eval's definition — the
  lifecycle function's own annotations and defaults are documentation, never
  contract (``_state: STATE``, bare ``_state``, and ``_state: STATE = None``
  are equivalent spellings); names eval does not declare keep their own
  annotation (an extra injectable such as ``clock: CLOCK``); reading inputs
  stays stop-only. Signature resolution evaluates string annotations
  everywhere (``eval_str=True`` / ``inspect.get_annotations``, issue #83),
  so ``from __future__ import annotations`` modules wire identically. Input
  activity is REAL at the per-child link level: the node's start hook
  activates each packed input child per its ``active=`` policy and drops the
  framework's root subscription; the stop hook passivates every child (a
  stopped ``map_`` child must never be re-woken by a lingering subscription);
  the runtime scheduler remains the sole invocation gate. hgraph's runtime
  ``ts.make_passive()`` / ``ts.make_active()`` therefore work from Python
  node code (the ``until_true`` / ``freeze`` / ``take``-with-reset family).
  Activation itself never schedules or fabricates a modified tick. An active,
  already-valid ``REF`` argument requests one explicit startup sample because
  its initial binding predates graph start. Scheduler events still wake nodes
  declared with ``active=()``. ``NODE`` injects a callback-scoped projection
  over the same native ``NodeView`` used by C++ nodes, including identity,
  graph identity, kind, lifecycle state, input/output presence, and scheduling
  notification. Retaining that Python projection beyond the callback is an
  error; C++ node implementations receive the zero-storage borrowed
  ``NodeView`` directly;
- ``lift(fn, inputs=..., output=...)`` wraps a plain scalar function as a
  compute node (scalar annotations become ``TS[...]``; time-series views
  unwrap to ``value if valid else None`` before the call);
- ``lower(fn)`` performs the inverse boundary conversion through the native
  C++ ``LowerExecution`` path. Each reactive input is supplied as an
  Arrow-compatible frame and is replayed by ``from_data_frame``; output ticks
  are captured by ``to_data_frame`` and concatenated in C++. Scalar graph
  parameters remain wiring-time Python arguments. PyArrow is returned by
  default; supplying a Polars frame selects a Polars result. Python user nodes
  run under the same guarded runtime ``GlobalState`` projection as an ordinary
  graph run, and final state is copied back to the selected Python seed;
- the diagnostic sinks (``debug_print`` with ``sample=``, ``print_`` with
  python-style ``{}``/``{name}`` formatting and ``__std_out__``, the
  format-args ``assert_``) write through python's ``sys.stdout``/``stderr``
  via the bridge's writer hook, so redirection and pytest capture behave as
  in hgraph; ``DebugContext`` is wiring-scope sugar over ``debug_print``,
  and the ``LOGGER`` injectable resolves to the configured Python graph logger
  as a wiring-time object scalar. The executor owns a native spdlog logger for
  the run; the bridge sink forwards native ``log_``, trace, and runner messages
  to that same Python logger. ``logger_formatter`` receives node context for
  both Python-authored and native nodes. Plain-value keyword arguments to
  ``**kwargs``-collecting operators auto-lift to ``const`` ports on a
  resolution retry;
- ``@generator`` sources with captured scalar arguments, distinct state per
  wiring call, empty generators, exception propagation, and strictly increasing
  absolute output times;
- reference, subscription, and request/reply services implemented in Python,
  including path injection, scalar implementation configuration, and the
  existing multi-interface input/output API;
- Python service adaptors over the native keyed exchange; external push
  sources remain limited to the sanctioned graph-thread/cross-thread
  boundaries.

The compatibility gate intentionally does not recreate every Python-only
runtime mechanism.  ``REF`` remains an opaque value without ``.output``;
Python lifecycle callbacks are limited to wiring-time scalars and injectables;
service interfaces use one time-series request/subscription argument; and
custom engine control or specialized threading policies should be implemented
in C++ and exposed through the bridge. Constrained generic service interfaces
support explicit specialization through the native resolution/path model;
implicit specialization of a reference service with no typed client input is
necessarily unavailable. These are deliberate restrictions, not silent
fallbacks.

Recorded divergences / gaps (the morning-summary list):

- REF is **value-only** (Howard's ruling 2026-07-05): references are
  OPAQUE VALUES — store, emit, pass, compare (``ref.value``,
  ``TimeSeriesReference.make()`` for the empty reference) — but never
  dereferenced (no ``.output``). Code that needs the dereferenced value
  accepts it as an input: a ``REF[X]``-annotated node parameter receives
  the reference (plain ports promote to REF at the boundary); a non-REF
  parameter bound to a REF source receives the DEREFERENCED value
  (binding inserts the from-REF adaptation). Retargets follow the
  sampled contract — the new target's current value arrives as a tick.
- Python ``@graph`` functions are full ``WiredFn`` citizens (the ruled
  type-erased context+ops backend): ``map_``/``switch_`` COMPILE them as
  C++ sub-graphs, ``reduce`` accepts raw lambdas (un-annotated callables
  assume an output; only an explicit ``-> None`` marks a sink). Identity
  is the user function object; records are immortal (WiredFn contexts).
- **Python user nodes landed** (the ruling realised): ``@compute_node`` /
  ``@sink_node`` / ``@generator`` run Python functions as runtime nodes —
  graph-thread only, both modes, no side effects beyond their output. The
  GIL is RELEASED the instant the run loop starts; one nanobind guard covers
  the executor's complete start phase, each root evaluation cycle, and the
  complete stop phase (:doc:`python_bridge`, *GIL boundaries*). Inputs arrive
  as plain Python VALUES (a recorded
  divergence from Python hgraph's TimeSeries view objects); a compute
  node's return value ticks its output (``None`` = no tick); a generator
  yields ``(datetime, value)`` pairs emitted at their absolute times. The
  bridge registers internal erased operators (``__py_compute`` /
  ``__py_sink`` / ``__py_generator``) over an immortal callable-record
  scalar. Argument ports pack into ONE structural un-named TSB and
  wiring-time SCALARS ride a list-of-Any scalar, with a LAYOUT string
  (part of node identity) mapping the python call positions — any arity,
  one operator (Howard's review of the per-arity first cut).
  The internal wiring ``Port.node_type_info`` diagnostic reports the producing
  node record's family, role, kind, semantic label, implementation label, and
  ops ABI without introducing Python-only metadata.
  ``STATE`` / ``CLOCK`` / ``SCHEDULER``-annotated parameters are injected
  and MUST default to ``None`` (the hgraph convention, enforced at
  decoration - graph code never supplies them):
  STATE is a lazily-created per-node namespace preserved across ticks,
  CLOCK exposes ``evaluation_time``, SCHEDULER exposes
  ``schedule(datetime)`` / ``schedule_delta(timedelta)``. Each
  ``@generator`` call is a distinct source node.
- ``passive(port)`` landed (both languages): the feedback idiom is
  ``a + passive(fb())``, and such loops quiesce naturally. ACTIVE feedback
  consumption still needs an explicit end time.
- ``run_graph`` records graph outputs sparsely and reports their absolute
  engine timestamps. This is required for real-time graphs, whose wall-clock
  timestamps cannot be represented as a dense array starting at ``MIN_ST``.
  Simulation remains cycle-aligned in ``MIN_TD`` steps.
- ``@component`` + the record/replay modes are surfaced (all through the
  ``hgraph`` package — ``_hgraph`` is internal and never user-imported):
  ``record_replay_scope(RecordReplayEnum.RECORD | ...)`` is the context
  manager over the C++ RAII scope; the Python ``@component`` decorator
  replicates the C++ wrapping rules by name (Record / Replay /
  ReplayOutput / Recover / Compare); ``comparison_summary`` reads Compare
  results and ``frame_store_contains`` probes the store;
  ``recovering_pass_through`` is registry-wirable as
  ``__recovering_pass_through``. The eval_node/run_graph harness wires
  ungated ``__harness_record``/``__harness_replay`` aliases so the active
  record/replay MODEL never captures the test harness itself.
- **Python DSL frontier (end-game phase A1)**: the authoring surface now
  covers hgraph's ``_wiring`` test tier. Wiring-time input VALIDATION on
  python nodes rides the bridged C++ pattern matcher (``ResolutionScope``
  over ``ResolutionMap``/``ts_pattern_resolve`` — the single currency for
  py-side typevar resolution; ``IncorrectTypeBinding``/``ParseError``/
  ``RequirementsNotMetWiringError`` are ``WiringError`` subclasses), with
  three widening rules: ``TS[object]`` accepts any payload through the native
  ``Any`` type record (with an opaque Python-object fallback only when needed),
  ``tuple[E, ...]``
  re-matches fixed tuples through the C++ homogeneous-tuple pattern, and
  TSW strictness is deferred until the duration/tick marker lands. Plain
  values on TS params auto-lift to ``const`` (numeric scalars are
  PYTHONIC-strict: strings never coerce; a const of the DELTA shape
  applies as the initial tick). ``AUTO_RESOLVE`` materialises resolved
  typevars/SIZE; ``valid=``/``active=``/``all_valid=`` accept name sets or
  wiring-time callables. ``all_valid`` is enforced by the native input view's
  recursive ``all_valid()`` operation, including TSL/TSB/TSD children.
  ``resolvers={...}`` binds typevars from scalars on compute, sink, graph,
  generator, component, service, adaptor, and push-queue declarations. TSS returns follow
  upstream exactly: an exact ``frozenset`` REPLACES the whole set, a
  ``set_delta``/``Removed``-marked set applies as a delta, and a net
  no-change on a valid output does NOT tick (``contains_`` re-publishes
  on item ticks only). TSB sugar: ``.as_schema`` (both wiring and runtime
  views), ``keys()``/``dict(**tsb)``, inline ``TSB["a": TS[int], ...]``
  schemas, ``TSL.from_ts(iterable | *ports, tp=...)``. The signature
  introspection surface (``WiringNodeSignature``/``extract_signature``/
  ``extract_kwargs`` + the node-class aliases) is PUBLIC hgraph exports —
  ``HgTypeMetaData`` is NOT part of the public API (raw annotations carry
  type info); ``const_fn`` is NOT ported (record_replay_table.rst P1).
- **Overload/dispatch core (end-game phase A2)**: python nodes/graphs are
  full operator-registry citizens. ``@operator`` roots an overload family
  (unique registry name per object); ``@compute_node/@sink_node/@graph/
  @generator`` accept ``overloads=`` (an @operator or a built-in operator
  family) and ``requires=`` (``lambda m[, <scalar names...>]``, ``m``
  keyed by type variable; rejection raises
  ``RequirementsNotMetWiringError`` — the C++ resolver throws a TYPED
  ``OperatorRequirementsError``, translated directly). Candidates register
  through ``register_python_overload`` as ordinary
  ``OperatorImpl{Source::Python}`` entries — the C++ registry's matching/
  ranking/normalisation own ALL dispatch; the wire closure re-enters the
  python wiring function under a borrowed ``Wiring``. Var-args nodes follow
  upstream's model exactly: the code object is rewritten to keyword-only
  parameters, ``*args`` packs into ONE TSL (or structural TSB when so
  annotated), ``**kwargs`` into ONE named TSB (or TSD); the node config
  carries a ``layout|names`` suffix and the kernel calls
  ``fn(*args, **kwargs)``. Ranking refinements: a plain value promoting to
  const is less specific than a true scalar parameter, and nested type
  variables decay (``TSL[~T, ~N]`` beats bare ``~T``). ``dispatch_``/
  ``@dispatch`` = key utility + enumerated ``switch_`` (design record:
  nested_graphs.rst). Python-class scalars and closed ``CompoundScalar``
  hierarchies both retain a concrete runtime type for dispatch.
- **Real-time + push sources** are surfaced with hgraph's shapes:
  ``run_graph(..., run_mode=EvaluationMode.REAL_TIME)`` runs the
  wall-clock executor (the GIL is released for the whole run), and
  ``@push_queue(tp, overloads=None, resolvers=None, requires=None, label=None,
  deprecated=False, conflate=False)`` wraps a function that IS the node's
  start lifecycle hook — called with the thread-safe sender callable
  (plus wiring-time scalars) once the graph runs; values convert
  schema-directed on the sending thread and cross the sanctioned C++
  boundary. Wiring the decorated function returns its port.
- ``GraphConfiguration`` exposes the upstream option names. ``run_mode``,
  start/end time, logger selection, default logger level, evaluation
  tracing/profiling, lifecycle observers, error capture, cleanup policy, and
  custom logger formatting are honoured. ``trace=True``
  or the upstream trace-options dictionary installs native ``EvaluationTrace``;
  ``profile=True`` or a dictionary installs native ``EvaluationProfiler`` and
  logs a formatted view of its owned snapshot. ``eval_node`` and ``run_graph``
  route their observer options through the same path. Custom Python lifecycle
  observers receive callback-scoped native ``Graph``/``Node`` views; retaining
  a view past the callback raises ``RuntimeError``. ``trace_back_depth`` and
  ``capture_values`` configure native uncaught-error diagnostics.
  ``cleanup_on_error=False`` retains the failed executor with the raised
  exception, deferring stop until that exception is released.
  ``trace_wiring=True`` or its ``filter``/``graph``/``node`` options dictionary
  installs the native ``WiringTracer``. ``wiring_observers`` currently accepts
  only that bound native tracer; Python-authored callback observers and the
  native event records are deliberately not exposed. No execution option is
  accepted and silently discarded.
  ``hgraph.debug.Inspector`` is likewise a bound native lifecycle observer.
  Python receives owned snapshots and may convert them to presentation rows;
  it does not walk runtime graph objects or implement inspection callbacks.
- The decorator ``node_impl=`` parameter is present for signature compatibility
  but deliberately rejects non-``None`` values. It selects an implementation
  class from the retired Python runtime; Python authors must provide the node
  callable directly and native implementations must use the C++ node API.
- **Stable Python ABI**: Wheels target the CPython 3.12 stable ABI
  (``cp312-abi3``), so one wheel per platform supports CPython 3.12 and
  later. Stable bridge builds require CMake 3.26 or newer for
  ``Development.SABIModule``; pure C++ builds retain the CMake 3.25 minimum.
- **Frame ↔ pyarrow**: Frames cross the boundary as ``pyarrow.Table``\ s
  through the Arrow C stream protocol (``__arrow_c_stream__`` capsules —
  zero copy, version-independent): ``frame_store_read`` returns Tables,
  Tables convert back to Frame values, and ``to_table``/``from_table``
  are fully usable from Python. The extension itself links against pyarrow's
  versioned Arrow libraries, so wheel build and runtime dependencies are
  constrained to the same supported ABI major (Arrow 24 for this release).
- **Contexts** are surfaced BOTH ways (Howard: existing python code must
  keep working). The hgraph-compatible API: ``with port:`` publishes (the
  wiring port is a context manager; ``as name`` binds the context name via
  frame-local resolution), and a ``CONTEXT[TS[X]] = None`` parameter on a
  user node resolves by type/name — ``REQUIRED`` / ``REQUIRED["name"]`` /
  a call-site ``context="name"`` override, ``WiringError`` on failure; the
  type-based compatibility check normalises parameterised Python aliases to
  their origin class, so a published ``TSB[Price[float]]`` satisfies a context
  requested through a non-generic base class of ``Price``; the resolved
  context VALUE is entered (context-manager protocol) around each
  eval, exactly hgraph's semantics. The C++-design-record API also stands:
  ``with hg.context("name", port)`` + ``hg.context.get/has`` over the
  string-keyed scope stack. Underpinning both: **arbitrary python objects
  are first-class scalars** — ``TS[AnyClass]`` maps onto the new
  ``object`` value kind (a GIL-safe refcounted ``PyObj`` scalar; value ops
  acquire the GIL around refcount changes since the run loop releases it),
  and ``const`` infers it without ``tp=``.

- **Global run state** preserves the C++ ownership model.  Python keeps one
  ``GlobalState`` seed per thread.  ``GlobalContext`` selects that seed for an
  outer wiring/run scope and rejects nesting; ``with GlobalState()`` is
  compatibility shorthand.  A top-level Python ``Wiring`` copies from the
  selected seed, the C++ builder and root graph then use their normal owned-copy
  lifecycle, and the bridge replaces the Python seed with the root graph's final
  state after execution.  Runtime access is explicit: a graph or node declares
  the ``GlobalState`` injectable, and each callback receives a guarded projection
  built from its native ``GlobalStateView``. ``GlobalState.instance()`` is not a
  runtime lookup and raises during execution. The C++ graph never borrows Python
  storage.
- **Services** are surfaced per the runtime-identity rulings
  (services.rst *Runtime service identity*): ``@reference_service`` /
  ``@subscription_service`` / ``@request_reply_service`` decorate
  interface stubs (annotations give the schemas; calling the stub wires a
  client with ``path=``); implementations are
  ``@service_impl(interfaces=...)``-decorated (hgraph's shape — the
  declared interfaces validate the impl's signature per flavour at
  decoration and drive ``register_service(path, impl)``, path first;
  undecorated impls are refused). Generic interfaces may carry decorator
  resolvers and unresolved implementations can be specialized at registration
  with ``resolution_dict=``; interfaces may be stubs or the NAMES of
  C++-defined interfaces (the ruled direction). MULTI-INTERFACE
  implementations work per the C++ ``register_services`` shape: the impl
  takes no wired inputs and uses ``impl_input(stub, path)`` /
  ``impl_output(stub, out, path)`` per interface inside its body (the
  erased ``service_impl_input``/``service_impl_output`` flows +
  ``register_multi_service_impl`` with the combined required-endpoint
  scope). The API is hgraph's exactly: the registered PATH is injected
  into an impl declaring a leading ``path: str`` parameter; inputs read
  via ``get_service_inputs(path, stub).ts`` (or
  ``stub.wire_impl_inputs_stub(path).ts``), outputs publish via
  ``set_service_output(path, stub, out)`` (or
  ``stub.wire_impl_out_stub(path, out)``). The erased core
  (``types/service_runtime.{h,cpp}``) shares
  the role markers, path grammar and node makers with the templates, so
  an erased registration UNIFIES with a template client on the same path
  (proven in ``test_service_runtime.cpp``). **Adaptors and mesh** are surfaced too:
  ``@adaptor`` stubs (first TS param = graph-side input, return = output,
  both optional), ``@adaptor_impl(interfaces=...)`` +
  ``register_adaptor(path, impl, resolution_dict=...)`` with impl-side ``from_graph``/
  ``to_graph`` — the four adaptor markers de-templated to roles like the
  services. ``@service_adaptor`` / ``@service_adaptor_impl`` extend that
  surface with the native per-client keyed request/reply exchange; one TS
  request and one TS response are supported, with bundles carrying multi-field
  protocols. ``mesh_(func, ...)`` constructs the mesh; inside its function,
  ``mesh_(func)[key]`` or ``get_mesh(func)[key]`` performs sibling lookup via
  a lazy ``MeshWiringPort``. The func's element type comes from the Python
  function's return annotation (records carry their annotated output schema).
- **TimeSeries view objects** (Howard's rulings: proper C++ objects, all
  kinds, strictly lazy): user nodes receive a C++-bound ``TimeSeries``
  view over the LIVE input — nothing converts unless accessed.
  Universal: ``.value`` / ``.delta_value`` / ``.modified`` / ``.valid`` /
  ``.all_valid`` / ``.last_modified_time`` / ``.owning_node`` /
  ``.owning_graph`` / ``.is_reference()``. Mutable ``_output`` views expose
  the same generic interrogation properties while retaining their native
  mutation API. Kind-dispatched: TSS
  ``added()``/``removed()``; TSD ``[]``/``keys()``/``modified_keys()``/
  ``modified_items()``/``removed_keys()``/``in``; TSL ``[i]``/``len``;
  TSB ``.field`` / ``[]``. Child access returns child views sharing the
  parent's lifetime guard: a view stored past its node's evaluation
  raises rather than dangling. ``delta_value`` builds hgraph's friendly
  shapes natively from the dict/set views (no canonical-delta
  intermediate). Python does not reproduce the old engine's endpoint topology
  mutation hooks: ``bind_output`` / ``un_bind_output``, their ``do_*``
  implementation hooks, ``re_parent``, direct parent/bound-output traversal,
  and subscription management remain native runtime responsibilities.
