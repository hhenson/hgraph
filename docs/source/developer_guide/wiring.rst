Wiring
======

Wiring is graph construction: it turns user-authored node and graph
descriptions into a runtime graph definition. For hgraph the **C++-first
static wiring** path is primary — a node is authored as a small C++ type and
assembled into a graph with the runtime builders. Python wiring remains a
authoring surface that *lowers* to the same runtime primitives.

.. seealso::

   *Graph Wiring* assembles authored nodes into a graph and resolves generic
   (type-variable) nodes at the ``wire<>`` call. *Operators* generalises that to
   **overload dispatch**: one logical name collecting many implementations, with
   the most specific selected at wiring time.

Authoring a node
----------------

A node implementation is a **stateless struct** with a static ``eval`` hook
(and optional static ``start`` / ``stop``). Its parameters are *selector*
markers (see *Schemas > Static Schema*) that pair a compile-time schema with a
runtime view:

.. code-block:: cpp

   #include <hgraph/types/static_node.h>

   struct AddOne
   {
       static constexpr auto name = "add_one";

       static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out)
       {
           out.set(in.value() + 1);
       }
   };

   struct Counter            // a source with node-local state
   {
       static constexpr auto name = "counter";

       static void start(State<Int> state) { state.set(Int{0}); }
       static void eval(State<Int> state, Out<TS<Int>> out)
       {
           const int next = state.get() + 1;
           state.set(next);
           out.set(next);
       }
   };

The selectors used at this layer are:

- ``In<Name, TS<T>>`` — a typed input. ``value()`` reads the current value,
  ``modified()`` / ``valid()`` report tick state.
- ``Out<TS<T>>`` — a typed output. ``set(v)`` writes the value and ticks the
  output at the current evaluation time.
- ``State<T>`` — a typed handle into node-local (value-layer) state.
- ``Scalar<"name", T>`` — a named scalar value used as wiring-time node or graph
  configuration; the scalar analog of ``In``.

Each parameter's **selector type** identifies its role, so the ``In`` / ``Out``
/ ``State`` / service parameters may appear in **any position** — the runtime
classifies them by type, not by where they sit. The node's *argument schema*,
however, is **ordered**: the relative order of ``eval``'s inputs defines the
node's arguments and is wired positionally or by keyword (the ``In<>`` name),
matching the equivalent Python signature (the two schemas must agree).
``start`` / ``stop`` differ — each takes only the subset it needs, matched by
name and validated by type. All hooks must be ``static`` and return ``void``,
and the implementation type must be empty/stateless.

Signature extraction
---------------------

``StaticNodeSignature<TImplementation>`` reflects the ``eval`` parameter list at
compile time and derives the runtime node contract. When an implementation
declares ``using signature_args = std::tuple<...>``, that tuple is the canonical
contract instead; this lets ``eval`` omit selectors that are used only by
``start`` / ``stop`` without removing them from the node schema:

- **input schema** — the ``In<>`` parameters become the fields of a non-peered
  input ``TSB`` (in argument order, by their ``Name``), including collection,
  ``SIGNAL`` and ``REF`` selectors;
- **output schema** — the single ``Out<>`` parameter's schema (at most one),
  including collection, ``SIGNAL`` and ``REF`` selectors;
- **state schema** — the ``State<T>`` parameter's value-layer schema;
- **scalar schema** — ``Scalar<"name", T>`` parameters become a scalar
  configuration bundle populated by ``wire<T>(...)`` / ``build_graph<G>(...)``;
- **recordable-state schema** — ``RecordableState<TSchema>`` replaces ordinary
  local state with a hidden output-backed state surface for system-level
  record/replay wiring;
- **node kind** — determined entirely from the node's shape: ``eval`` with
  ``In`` and ``Out`` → ``Compute``; ``Out`` only → ``PullSource``; ``In`` only
  → ``Sink``. ``PushSource`` is reserved for the specialized push-source
  node/builder implementation, which owns its message queue and sender; ordinary
  static nodes do not grow a generic ``apply_message`` operation;
- **input endpoint** — a ``TSEndpointSchema`` of ``non_peered(input_tsb, {
  peered(field)… })``, one peered terminal per ordinary input, with structural
  annotations for non-peered collection prefixes and nested endpoints.

Mapping onto the runtime (no parallel mechanism)
------------------------------------------------

``NodeBuilder::implementation<TImplementation>()`` is a **typed front-end over
the existing** ``NodeBuilder::native(...)``. It assembles exactly the triple
``native`` already consumes and delegates to it:

.. code-block:: text

   StaticNodeSignature<T>  ->  ( NodeTypeMetaData,        // kind + input/output/state schema
                                 NodeCallbacks,           // eval/start/stop thunks
                                 TSEndpointSchema )       // input peering annotation
                            ->  NodeBuilder::native(...)

The ``NodeCallbacks`` thunks are stateless lambdas that call
``static_node_detail::invoke<&T::eval, CanonicalArgs>(view, evaluation_time)``.
``invoke`` walks the hook's parameter types and, for each, asks an
``arg_provider`` to build the selector from the type-erased ``NodeView``.
Named input/scalar slots are located in ``CanonicalArgs`` rather than assumed to
match a hook-local ordinal:

- ``In<Name, S>`` ← ``view.input(t).as_bundle().field(Name)`` projected to the
  matching typed input view (``TS`` / ``TSS`` / ``TSD`` / ``TSL`` / ``TSW`` /
  ``TSB`` / ``REF`` / ``SIGNAL``)
- ``Out<S>`` ← ``view.output(t)`` projected to the matching typed output view
  (carrying ``t`` for mutation)
- ``State<T>`` ← ``view.state()``
- ``RecordableState<TSchema>`` ← ``view.recordable_state(t)``
- ``Scalar<"name", T>`` ← the node's scalar configuration bundle
- ``GlobalStateView`` / ``EvaluationClockView`` / ``EngineControlView`` /
  ``NodeScheduler`` ← graph and node runtime injectables

Because the static layer produces the same ``native`` inputs the hand-written
path uses, there is **one** runtime node model; static authoring is sugar over
it, not a second mechanism.

Assembling a graph
------------------

Nodes are assembled with ``GraphBuilder`` and connected with ``GraphEdge``
(``source_node`` / ``source_path`` → ``target_node`` / ``target_path``).
``source_node`` is a plain node index for ordinary output edges. Special source
roots use ``make_graph_edge_source(node, kind)`` to pack ``GraphEdgeSourceKind``
into the high bits of the same word. Paths index into ``TSB`` / ``TSL`` structure
below the selected source root. An input ``target_path`` of ``{0}`` selects the
first field of the node's input bundle.

.. code-block:: cpp

   GraphBuilder builder;
   builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
          .add_node(NodeBuilder{}.label("inc").implementation<AddOne>())
          .add_edge(GraphEdge{.source_node = 0, .source_path = {},
                              .target_node = 1, .target_path = {0}});

   GraphExecutorBuilder executor;
   executor.graph_builder(std::move(builder)).start_time(MIN_ST).end_time(end);
   executor.make_executor().view().run();

Project Boundary
----------------

Python wiring remains a compatibility goal for the existing hgraph ecosystem.
When built, it must *lower* into the same runtime construction data the static
path produces (``NodeTypeMetaData`` + ``NodeCallbacks`` + ``TSEndpointSchema``
via ``native``), so the runtime never depends on Python wiring for
correctness. The runtime should validate the invariants that protect memory
safety, and trust the type-checking done at the wiring layer.

Status
------

Implemented: static C++ nodes support ``In`` / ``Out`` selectors over ``TS``,
``TSS``, ``TSD``, fixed and dynamic ``TSL``, ``TSW``, ``TSB``, ``REF`` and
``SIGNAL``; ``State<T>``; ``RecordableState<TSchema>``; wiring-time
``Scalar<"name", T>`` arguments; ``GlobalStateView`` / ``EvaluationClockView`` /
``EngineControlView`` / ``NodeScheduler`` injectables; input ``InputActivity`` / ``InputValidity``
policy flags; ``TsVar`` / ``ScalarVar`` generic node resolution; and
``StaticNodeSignature`` / ``NodeBuilder::implementation<T>()`` over
``start`` / ``stop`` / ``eval`` hooks. Node-kind inference covers Compute /
PullSource / Sink from shape with no override. ``GraphBuilder`` /
``GraphEdge`` and the higher-level ``Wiring`` / ``wire<T>`` / ``wire<G>`` /
``build_graph<G>`` layer are implemented for flattened graphs, including
by-name node/graph wiring arguments, node-level scalars, scalar defaults,
graph-level scalar parameters, structural TSL/TSB input
initialisers, special source roots (``recordable_state(port)`` /
``error_output(port)``), and ordinary simulation execution. Also implemented
since this page was first written: push-source nodes with real-time execution
(``runtime/push_source_node.h`` — ``make_push_source_node`` with Queue /
Conflating policies and a thread-safe ``PushSourceSender`` handed to user code
at ``start``), standalone sub-graph boundary binding (``compile_subgraph<G>``),
feedback edges, and the higher-order graph operators ``map_`` / ``reduce`` /
``switch_`` / ``mesh_`` (see :doc:`nested_graphs` and :doc:`mesh`), plus
services / shared outputs / the context runtime primitive (see :doc:`services`).

Deferred (land with the relevant runtime layer):

- automatic recordable-state recording;
- static-node authoring sugar for push sources (the runtime node/builder above
  is the supported path today);
- named state (``State<TSchema, "name">``);
- generic (``TsVar``) sub-graphs;
- the Python lowering.
