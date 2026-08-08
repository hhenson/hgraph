Graph Wiring
============

This page describes how C++ graph wiring is intended to work, and how the same
engine will back Python graph wiring. The goals are: wiring that reads like node
wiring (see *Wiring*), runs **at wiring time** (not during evaluation), and shares
its core with the Python bridge so a Python ``@graph`` and a C++ graph build the
same runtime graph the same way.

.. note::

   **Status.** Implemented: the ``Wiring`` core (interning + topo-sort/rank), the
   typed ``Port<Schema>`` + ``wire<T>`` facade for nodes, ``build_graph<G>(…)`` for
   a top-level graph, **sub-graph composition** (``wire<G>`` inlines a graph, with
   the same compile-time argument checking and scalar-literal auto-wrapping as
   ``wire<T>``), **scalar inputs** (``Scalar<>`` arguments folded into the intern
   key and recorded on the node builder), ``StaticGraphSignature<G>`` with
   **graph-level scalar parameters**, **structural (non-peered) sources** — brace
   initializers ``wire<T>(w, {a, b})`` (positional and by-name) and the ``to_tsl``
   / ``to_tsb`` collection builders, including adapting a structural source onto a
   ``REF`` input — **generic (type-variable) nodes and operator-overload dispatch**
   (see *Operators*), and a first **non-flattening nested-graph node**
   (``single_nested_graph_node``; see *Nested graphs*). Code in
   ``include/hgraph/types/graph_wiring.h`` + ``src/hgraph/types/graph_wiring.cpp``
   (and ``include/hgraph/runtime/nested_graph_node.h`` + its ``.cpp``), with
   ``tests/cpp/test_graph_wiring.cpp``. The ``wire``-level higher-order operators
   (``map_`` / ``reduce`` / ``switch_`` / ``mesh_``), standalone sub-graph
   building with supplied time-series boundary ports (``compile_subgraph<G>`` —
   see slice 9 below), feedback edges, and wiring-only ``delayed_binding``
   placeholders have all since landed; see
   :doc:`nested_graphs` and :doc:`mesh` for their design records, with the
   corresponding slices of this page as the wiring-level record. The user-facing
   view is *User Guide > Wiring Graphs in C++*.


Two tiers
---------

Because Python cannot instantiate C++ templates, the engine is split so that the
part Python shares is an ordinary runtime object, and the C++ ergonomics sit on
top of it:

.. mermaid::

   flowchart TD
      cpp["C++ graph-struct<br/>StaticGraphSignature&lt;G&gt; (reflect wire())"]
      py["Python @graph<br/>(signature introspection — planned)"]
      core["shared runtime core<br/>(Wiring → GraphBuilder)"]
      rank["topo-sort + rank"]
      flat["flattened nodes + edges"]
      rt["runtime graph"]

      cpp --> core
      py --> core
      core --> rank --> flat --> rt

- **Runtime core** (``Wiring``) — a small, language-agnostic object that
  accumulates a graph and, on finish, topologically sorts and ranks it. Both C++
  and Python call into this.
- **Typed C++ facade** — ``Port<Schema>``, ``wire<T>(...)``, ``wire<G>(...)`` and
  the graph-as-struct form. Adds compile-time checking and *lowers to* core
  calls.

A graph in either language is the same thing: a named **signature**
``(time-series inputs) -> time-series output(s)`` whose body wires sub-nodes and
sub-graphs, lowering to identical core calls. The signature is the shared
contract; only how it is recovered differs (C++ reflection vs Python
introspection).


Identity at wiring time, rank at build time
-------------------------------------------

A wired node cannot be given its final ordering index while wiring, because the
final index *is* its rank, and rank is only known once the whole graph exists. So
identification and ranking are separate steps:

- **Wiring identifies — and interns.** Each ``add_node`` forms a **wiring
  instance** — the node *definition* plus its inputs (the time-series input ports
  **and** the scalar input values) — and **dedups** it: an equivalent instance
  returns the existing one; otherwise the new instance is added. Because instances
  are interned with a **stable address**, the ``WiringInstance`` pointer *is* the
  node's wiring-time identity — ports and edges reference that pointer, never a
  final index; insertion order is irrelevant. Two instances are equivalent when
  the node definition matches **and all inputs are equal** — time-series inputs
  (ports) are equal when they reference the same producing ``WiringInstance`` and
  path; scalar inputs are equal by value. Because producing instances are
  themselves interned, the keys are canonical and the dedup is transitive across
  the whole graph: this is wiring-time common-subexpression elimination, the
  node-level analogue of the structural interning of plans/schemas/ops (see *Data
  Structures > Core Concepts*).

  A wiring instance is the closest analogue of a Python node *instance*. The node
  **definition** is identified by its *type* — ``typeid(T)`` for a C++ static
  node, the node class in Python — and **not** by the runtime ``NodeBuilder``,
  which is a per-construction build artifact (two builds of the same node type are
  not the same object). The definition is shared and carries no per-instance
  scalars or ports; the **instance** adds the scalar values and input ports that
  distinguish it. In the scalar-aware design the runtime ``NodeBuilder`` is built
  at ``finish`` from the definition plus the instance's scalar values; the current
  slice has no scalars yet, so it builds the (scalar-free) ``NodeBuilder`` eagerly
  and stores it on the instance.
- **Build ranks.** Finishing runs the rank pass: place source nodes in the
  prefix (and set ``push_source_nodes_end``), topologically order the rest so
  ``rank(parent) < rank(child)``, assign ``final_index = rank``, and remap every
  edge's ``WiringInstance`` endpoints to final indices (via a ``WiringInstance* →
  index`` map).

The **runtime is unchanged** by this: it still evaluates in index order and relies
on ``rank(parent) < rank(child)``; that invariant is now *produced* by the rank
pass rather than being the caller's responsibility (which is the fragility behind
add-order-equals-rank-order assumptions). ``feedback`` is the explicit exception:
its one-cycle-delayed edge does not constrain rank. A ``delayed_binding`` is not
such an exception; it resolves to an ordinary source before ranking and therefore
cannot create a cycle.


The shared core
---------------

.. code-block:: cpp

   // Implemented shapes. The typed facade Port<Schema> is below.
   struct WiringInstance;

   // Erased handle to a time-series source. A *peered* source names a producing
   // (interned) instance, a source root (ordinary output, error output, or
   // recordable-state output), and an output path within that root. A *structural*
   // source has no producer of its own — its children are the sources for each
   // fixed child slot (to_tsl / to_tsb and brace initializers build these). A
   // *delayed* source carries shared wiring-only binding state plus a projection
   // path until its producer is supplied. A *null* source is an unbound fixed
   // slot carrying only its schema. Target info lives on WiringInputRef, never
   // here.
   struct WiringPortRef {
       struct PeeredSource {
           const WiringInstance *node;
           std::vector<std::size_t> path;
           GraphEdgeSourceKind output_kind;
       };
       struct StructuralSource { std::vector<WiringPortRef> children; };
       struct DelayedSource {
           std::shared_ptr<WiringDelayedBindingState> state;
           std::vector<std::size_t> path;
       };
       enum class SourceKind { Unbound, Null, Peered, Structural, Boundary, Delayed };

       // Wiring-time argument adornment (Python's pass_through()/no_key()
       // map_ wrappers): consumed by the operator that receives the port,
       // NEVER part of graph structure — edge/source interning ignores it
       // (operators that must not dedup across tags fold them into their
       // scalar identity, e.g. map_'s MapCallConfig).
       enum class ArgTag : std::uint8_t { None, PassThrough, NoKey, Passive };

       const TSValueTypeMetaData *schema;
       ArgTag                     arg_tag;  // with_arg_tag(tag) returns a tagged copy

       // factories peered_source / structural_source / null_source, plus
       // source_kind() and the typed accessors.
   };

   // Consumer-side input edge: a source port plus the (optional) target path on the
   // consuming node it binds into.
   struct WiringInputRef { WiringPortRef source; std::vector<std::size_t> target_path; };

   // The interned wiring identity (stable address, so WiringInstance* IS the
   // identity). The NodeBuilder carries any per-instance scalar configuration
   // (set by add_node). Edges are derived from `inputs` at finish.
   struct WiringInstance {
       NodeBuilder                 builder;   // build artifact (carries scalars)
       std::vector<WiringInputRef> inputs;    // time-series input edges
   };

   class Wiring {
     public:
       // `def` is the node definition's identity (typeid(T) for a C++ static node):
       // two calls with the same def + equal inputs + equal scalars dedup. `builder`
       // is stored for finish; the `scalars` value is recorded on it (empty if none).
       // Output-less (sink) nodes are never deduped — each must run for its own side
       // effect — so only value-producing nodes are interned.
       WiringPortRef add_node(std::type_index def, NodeBuilder builder,
                              std::span<const WiringInputRef> inputs, Value scalars);
       WiringPortRef add_node(std::type_index def, NodeBuilder builder,   // positional overload
                              std::span<const WiringPortRef> inputs, Value scalars);
       // Topo-sort + rank → a rank-ordered GraphBuilder (edges from each instance's inputs).
       GraphBuilder  finish() &&;
   };

**The ``Passive`` tag** is the general-purpose marker (Python's
``passive(ts)``, C++ ``passive(port)``): a tagged COPY of a port whose
receiving node drops the matching slot from its active list — ticks on
that input no longer schedule the node, values still read normally. The
adjustment happens in ``Wiring::add_node`` via
``NodeBuilder::with_passive_inputs`` (a schema rebind, like error capture —
native nodes only), BEFORE schema resolution, so node identity/interning
reflects passivity. Marking every input passive throws (the node could
never fire). The motivating idiom is the feedback read —
``add_(ts, passive(fb()))`` — which lets a bound feedback loop quiesce
instead of re-ticking forever.

**A delayed binding changes compose order, not evaluation order.**
``delayed_binding<S>(w)`` owns shared control state. Atomic and dynamically
shaped schemas expose one ``DelayedSource``. Fixed ``TSL`` and ``TSB`` schemas
instead expose a structural tree whose leaves are delayed sources, so consumers
select the correct non-peered endpoint layout immediately. Binding a peered
aggregate projects it onto those leaves; binding a structural aggregate pairs
the leaves recursively. ``TSD`` key-set views append their component to the
single delayed source's projection path. ``finish`` resolves every delayed
source, then performs the normal producer collection, ranking, and edge emission.
An unbound handle, incompatible schema, duplicate binding, or dependency cycle is
a wiring error. No delayed source, binding state, node, or extra tick reaches the
runtime graph.

The consumer endpoint schema is fixed when the consumer node is added. A
structural source whose cardinality is not known from the declared schema, such
as a dynamic ``TSL`` assembled from independent ports, must therefore first be
materialized as a peered output. Fixed ``TSL``/``TSB`` structural sources,
their projections, and key-set projections from a delayed ``TSD`` are supported.

- ``add_node`` keys the instance on ``(def, resolved schemas, input edges,
  scalars)`` — ``def`` is the node type's identity (``typeid(T)``), the resolved
  input/output/scalar/state schema pointers distinguish different generic
  resolutions of one definition, and ``scalars`` is the compound scalar
  configuration value (empty when the node has no scalar inputs) — and looks it up
  in the wiring intern table. **Output-less (sink) nodes skip interning**: two
  identical sinks must stay distinct so each performs its side effect (matching
  Python, which never CSE's node calls). On a hit it returns the existing
  instance's port; on a miss it records the ``scalars`` on the ``NodeBuilder``,
  interns the new instance, and returns a ``WiringPortRef`` to its output. No edges
  are recorded yet — the input edges carry the dependencies.
- ``finish`` runs the rank pass, then walks the instances in rank order, adding
  each instance's ``NodeBuilder`` to a ``GraphBuilder`` and emitting the edges for
  each input: a **peered** input is one edge (source node/path → this node's input
  path); a **structural** input expands to **one edge per peered leaf**, each into
  the matching child slot of the input (a null leaf emits no edge). Endpoints are
  remapped from ``WiringInstance*`` to final indices. The ``NodeBuilder`` has no
  knowledge of edges; they are applied by the graph builder.
- The core is language-agnostic, so the Python wiring bridge reuses the interning
  and rank pass — including the scalar values in the key.


The typed C++ facade
--------------------

- ``Port<Schema>`` is the typed handle; ``.erased()`` lowers it to a
  ``WiringPortRef`` (the runtime schema comes from ``Schema``). A sub-graph
  input parameter declared as ``Port<SIGNAL>`` is treated as a tick subscription:
  any upstream time-series output port may be supplied, regardless of its value
  schema.
- ``wire<T>(w, args...)`` — takes the node's wiring arguments positionally or by
  keyword with ``arg<"name">(value)``. Names target the node's ``In<Name, ...>``
  and ``Scalar<Name, ...>`` selectors; positional arguments fill the eval-parameter
  order. It checks **arity and, per parameter, the port schema or scalar convertibility** at
  compile time (via ``StaticNodeSignature<T>``, which exposes the output schema
  type, the input schema types and the ordered list of wiring parameters), splits
  the arguments into the time-series input ports and a compound scalar value, lowers
  to ``w.add_node(typeid(T), builder, ports, scalars)``, and returns
  ``Port<output_schema_type>`` (or ``void`` for a sink). The scalar value is built
  as the un-named bundle ``StaticNodeSignature<T>::scalar_schema()`` describes.
- ``wire<G>(w, args...)`` — inlines ``G::compose(w, …)`` (graphs flatten) and
  returns ``G``'s output port. The arguments follow the **same rule as for a node**
  (via ``StaticGraphSignature<G>``): positional arguments fill compose-parameter
  order, while keyword arguments target ``NamedPort<Name, S>`` and
  ``Scalar<Name, T>`` parameters. Port arguments are schema-checked and scalar
  arguments are wrapped into their ``Scalar<>`` selectors, so a sub-graph and a node
  are wired identically at the call site. An erased generic-source port is checked
  against the declared sub-graph input schema before it is retyped for ``compose``.
- **Scalar arguments unpack uniformly.** Every scalar wiring argument (in
  ``wire<T>``, ``wire<G>`` and ``build_graph``) passes through one helper,
  ``graph_wiring_detail::coerce_scalar_value<V>``: it accepts either a plain value or
  a ``Scalar<Name, T>`` **selector** (whose value it unpacks), so a scalar received
  as a node/graph parameter can be forwarded straight on — ``wire<Shift>(w, x, by)``
  rather than ``…, by.value()``. Only the value type must be convertible; the
  ``Name`` need not match (the producer's and consumer's scalar names are
  independent).
- ``StaticGraphSignature<G>`` — **implemented.** Reflects ``&G::compose``
  **skipping the leading ``Wiring&``**: ``Port`` parameters are the graph's
  time-series inputs, ``Scalar`` parameters are its scalar inputs, and the return
  type is its time-series output(s). It exposes ``param_types`` (the ordered
  parameter selector list), ``output_type``, and ``param_count`` / ``input_count``
  / ``scalar_count``. This is the graph-level mirror of ``StaticNodeSignature``.
- ``build_graph<G>(values...)`` — **implemented for a top-level graph.** Constructs
  a ``Wiring``, wraps each supplied plain value into the corresponding
  ``Scalar<>`` ``compose`` parameter (checked against ``StaticGraphSignature<G>``:
  ``input_count() == 0`` — a top-level graph has **no** time-series inputs or
  outputs — and the argument count matches ``scalar_count()``), calls
  ``G::compose(w, scalars…)``, and returns ``w.finish()``. Supplying time-series
  boundary input ports (for standalone sub-graph building) is a later slice.

The compile-time checks are the C++ advantage over Python; the core re-validates
schemas at wiring time as a safety net (and as the only check Python relies on).

.. note::

   The graph's body method is named ``compose`` and the wiring verb is ``wire`` —
   distinct names — so inside a ``compose`` body you call ``wire<…>`` directly,
   without qualification.


Graphs flatten
--------------

Graph composition is **inlining**: ``wire<G>`` expands ``G``'s body into the
current ``Wiring``; only nodes become runtime objects. This matches Python, where
``@graph`` functions are wiring-time only and disappear into nodes.

Nested graphs that must **not** flatten — ``map_`` / ``reduce`` / ``switch_`` and
other higher-order operators — own a child graph at runtime rather than inlining.
The runtime substrate exists (see *Nested graphs* below) and the wiring surface
that compiles a sub-graph into it — ``compile_subgraph<G>`` / ``nested_<G>`` — is
done; the design record for that layer and the higher-order operators is the
dedicated *Nested Graphs* page.


Nested graphs
-------------

A **nested-graph node** owns and drives one child graph instead of flattening it.
It is built outside the ``Wiring`` flatten path, via
``single_nested_graph_node(meta, spec)``
(``include/hgraph/runtime/nested_graph_node.h``), and plugs into the generic node
through the node extension points: a ``NodeStorageField`` for the child
``GraphValue``, a custom ``NodeOps`` for ``evaluate`` (plus ``start`` / ``stop``
callbacks), and a ``NodeView::as<SingleNestedGraphNodeView>()`` extension view.

- **Boundaries are reference-bound, not copied.** ``spec.input_bindings`` map a
  path in the outer node's input to a child node's input endpoint; each cycle the
  child input is bound to the *same upstream output* the outer input is bound to
  (``bind_output``). ``spec.output_binding`` forwards a child node's output through
  the outer node's **forwarding output** (its ``output_endpoint_schema`` is
  ``peered``), so the outer output shares identity with the child's rather than
  copying values. Re-binding each cycle is cheap — skipped when the endpoint
  already references the same output — and absorbs an upstream ``REF`` that
  re-points.
- **Lifecycle.** The child graph is instantiated lazily, started/stopped with the
  node (configurable via ``SingleNestedGraphNodeOptions``), and the child's next
  scheduled time is propagated up so the parent re-evaluates when the child has
  pending work. The forwarding link is left intact on stop — the child output lives
  in the node's storage and is torn down with the parent output, so the forwarded
  value stays observable after a run; ``switch_``-style wrappers that swap the
  active child use ``single_nested_graph_clear_output_binding``.
- ``SingleNestedGraphNodeView`` is reusable scaffolding: policy wrappers
  (``try_`` / ``switch_`` / delayed component) can supply their own callbacks while
  reusing the same storage and binding model.

Implemented today: a single child and output forwarding at the output root only.
Child graphs are built either by hand (``GraphBuilder``) or — the normal path —
compiled from a sub-graph ``compose`` via ``compile_subgraph<G>`` and wired with
``nested_<G>`` (see *Nested Graphs*). The multiplexing operators (``map_`` /
``reduce`` / ``switch_``) build on the same artifacts and are specified there.


Extending to Python
-------------------

Python wiring drives the **same core**:

- At wiring time a Python ``@graph`` runs its body; calling a node builds a
  ``NodeBuilder`` through the Python→C++ bridge and calls ``Wiring::add_node``;
  calling a sub-graph inlines its wiring; finishing runs the same rank pass.
- Ranking, flattening and graph construction are therefore single-sourced in C++;
  Python never sees node indices.
- The graph **signature** is the shared contract: C++ recovers it with
  ``StaticGraphSignature`` reflection, Python with function-signature
  introspection; both yield the same ``(TS inputs) -> TS output`` shape and the
  same sequence of core calls.
- The explicit ``Wiring&`` first parameter in C++ corresponds to Python's
  implicit *current graph* context; it is an implementation detail that the C++
  signature reflection ignores, so the logical signatures line up.


Wiring diagnostics
------------------

``WiringObserver`` is the native observation point for graph construction. A
top-level C++ graph installs borrowed observers with
``build_graph_with_observers<G>(observers, ...)``; code that constructs a
``Wiring`` directly uses ``Wiring::add_wiring_observer``. The observer must
outlive the wiring operation.

Graph, nested-graph, and node callbacks receive ``WiringScopeEvent``. Overload
selection receives ``WiringResolutionEvent`` with the selected candidate,
rejected and ambiguous candidates, effective ranks, implementation source, and
rejection reasons. Paths and labels are owned strings. ``WiringTypeHandle``
refers only to registry-interned schema metadata, so a copied event remains safe
to inspect after the temporary ``Wiring`` and completed graph build are gone; no
``WiringInstance`` or runtime graph pointer crosses this API.

Child-graph compilation calls ``Wiring::child_wiring()``. This explicitly shares
the observer registry and copies the current path into the child, covering
``switch_``, ``map_``, ``mesh_``, ``reduce``, and other compiled callables
without a process global or thread-local observer stack. Callbacks are
synchronous and observer exceptions abort wiring. ``WiringTracer`` is the
built-in native formatter and accepts path, graph-event, and node-event filters.
The observer and event interfaces are C++ APIs. Python may configure the bound
native ``WiringTracer``, but cannot implement an observer or receive the event
records through Python callbacks.


Status and roadmap
------------------

Slices:

1. **Done.** The shared ``Wiring`` core (``WiringInstance`` interning by
   ``(typeid(T), input ports)`` + Kahn topo-sort/rank), the ``StaticNodeSignature``
   extension exposing the output schema type, the typed ``Port<Schema>`` +
   ``wire<X>(w, …)`` facade (compile-time arity **and per-port schema matching**),
   ``build_graph<G>()`` for a top-level graph, and **sub-graph composition** —
   ``wire<X>`` dispatches on
   whether ``X`` is a node (``eval``; adds a runtime node) or a graph (``wire``;
   inlined and flattened). Tests: ``tests/cpp/test_graph_wiring.cpp``.
2. **Done.** Scalar inputs: ``wire<T>`` takes ``Scalar<>`` arguments positionally
   or by selector name, builds the compound scalar value, **folds the scalar
   values into the intern key** (so equal scalars dedup, distinct scalars do not),
   and records them on the ``NodeBuilder``. Node-layer scalar storage and the
   ``Scalar<>`` authoring selector back it (see *Authoring Nodes in C++*).
3. **3a — Done.** ``StaticGraphSignature<G>`` (reflects ``compose`` skipping
   ``Wiring&``; classifies ``Port`` vs ``Scalar`` parameters; exposes the output
   type) and **graph-level scalar parameters**: ``build_graph<G>(values…)`` wraps
   the supplied values into the graph's ``Scalar<>`` ``compose`` parameters and
   forwards them. Scalar defaults are supplied by ``static defaults()`` or
   ``signature_defaults<T>``, both returning ``std::tuple{arg<"name">(value)...}``.
   Tests: ``tests/cpp/test_graph_wiring.cpp``.
4. **3b — Done.** ``wire<G>`` parity with ``wire<T>``: the sub-graph branch now
   reflects ``compose`` via ``StaticGraphSignature<G>``, checks argument arity and
   per-parameter port schema / scalar convertibility at compile time, supports
   ``arg<"name">(...)`` for ``NamedPort`` / ``Scalar`` parameters, and
   **auto-wraps scalar literals** into the sub-graph's ``Scalar<>`` parameters.
   A sub-graph and a node are now wired identically at the call site. Tests:
   ``tests/cpp/test_graph_wiring.cpp``.
5. **Generic (type-variable) nodes — done.** A node authored over ``TsVar`` /
   ``ScalarVar`` (one implementation, no per-type instantiation) is resolved to a
   concrete node at the ``wire<>`` call. ``wire<>`` builds a ``ResolutionMap``
   (``include/hgraph/types/type_resolution.h``): each input selector's pattern is
   **unified** against the connected port's runtime schema, a scalar variable is
   inferred from the configured value's type, and a source-side output variable is
   supplied **explicitly** — either ``wire<replay, TS<Int>>(w, key)`` (an
   explicit output schema, which also makes the returned port the typed
   ``Port<TS<Int>>``) or
   via ``ts_type<...>()`` / ``scalar_type<...>()`` helpers. The resolved schemas
   build the node through ``NodeBuilder::implementation<T>(map)``. When a generic
   output type is only known at wiring (resolved from inputs/values, not supplied),
   ``wire<>`` returns the **erased** ``Port<void>`` carrying the runtime schema;
   downstream ``wire<>`` accepts it (typed or erased). The resolved schema pointers
   are part of the node's interning key, so distinct resolutions of one definition do
   not collide. The framework's own ``replay`` / ``record`` / ``const_`` /
   ``debug_print`` / ``null_sink`` are authored this way. Tests:
   ``tests/cpp/test_type_resolution.cpp``.

   *Operator overload dispatch* — one logical name (``add_``) collecting many
   implementations and selecting the most specific at the ``wire<>`` call — builds
   directly on this generic-resolution machinery (same ``ResolutionMap``, same
   ``NodeBuilder::implementation<Impl>(map)``) and is specified on its own page,
   *Operators*. It is the multi-candidate generalisation of the single-implementation
   resolution above.
6. **Structural (non-peered) sources — done.** Brace initializers
   ``wire<T>(w, {a, b})`` (positional → fixed TSL/TSB) and ``{{"f", a}, …}``
   (by-name → TSB fields, missing fields filled with null sources), plus the
   ``to_tsl`` / ``to_tsb`` collection builders, produce a structural
   ``WiringPortRef`` whose peered leaves expand to one edge each at ``finish``. A
   structural source bound to a ``REF`` input is adapted through a synthetic
   reference node. Tests: ``tests/cpp/test_graph_wiring.cpp``.
7. **Special source roots — done.** ``recordable_state(port)`` and
   ``error_output(port)`` expose a node's hidden recordable-state or error output
   as explicit C++ wiring ports. They emit a packed ``GraphEdgeSourceKind`` in
   ``GraphEdge::source_node`` rather than sentinel path entries, so ``source_path``
   remains only a structural path below the selected root and ordinary output edges
   keep the original edge footprint. These helpers are intentionally C++ wiring
   APIs; they are not Python-exposed user syntax.
8. **Nested-graph node — first cut done.** ``single_nested_graph_node`` owns one
   child graph and reference-binds its boundaries (see *Nested graphs*). Tests:
   ``tests/cpp/test_graph_wiring.cpp``.
9. **Standalone sub-graph building — done.** ``compile_subgraph<G>`` compiles a
   sub-graph ``compose`` against boundary placeholder ports (a wiring-only
   ``WiringPortRef`` source kind — no stub nodes) into a ``CompiledSubGraph``;
   ``nested_<G>`` wires it as a non-flattening nested-graph node. Design record:
   *Nested Graphs*. Tests: ``tests/cpp/test_nested_wiring.cpp``.
10. **Next.** The higher-order operators (``switch_`` / ``map_`` / ``reduce``)
    that build on ``CompiledSubGraph`` (roadmap on the *Nested Graphs* page), and
    feedback edges.

Deferred: multiple outputs (``TSB`` ports, optionally returned as an array as sugar);
**graph-level** generic resolution (``TsVar`` / ``ScalarVar`` in a *graph*
``compose`` signature — node-level resolution above is done); higher-order operators
and feedback; dead-node pruning; and the Python bridge that drives the core.
