Node Schemas
============

A node schema describes the contract of a single graph node — what it
exposes to the wiring layer, what lifecycle hooks it supports, and what
runtime state it requires. The schema itself owns no executable code
and no memory layout: it is a pure description that the
*Allocation, Plans and Ops* layer turns into a concrete runtime node.

A node, viewed through the Plan / Schema / Ops / Builder / Value / View
vocabulary, is the next layer up from a time-series:

================  ==========================================================
Concept role      Node-layer name
================  ==========================================================
Schema            ``NodeTypeMetaData`` — kind, input/output/error/state
                  schemas, scheduler/error flags, scalar configuration,
                  and input readiness selectors.
Plan              ``StoragePlan`` — memory layout for the node's runtime
                  state as produced by ``node_storage_plan_for``: runtime
                  header, input/output endpoints, state, scalars,
                  optional scheduler, optional error output, optional
                  recordable-state output, plus any extra fields supplied
                  by a specialised node builder.
Ops               ``NodeOps`` — the behaviour vtable: ``start``,
                  ``eval``, ``stop``, endpoint/state projections,
                  scheduler access, graph attachment, and cleanup. Each
                  op receives an erased context pointer plus the node's
                  runtime memory/view. Construction and destruction are
                  handled by the bound ``StoragePlan`` lifecycle hooks,
                  not by ``NodeOps``.
Type record       ``TypeRecord`` — canonical interned ``Node/Runtime``
                  identity selecting the schema, plan, and ``NodeOps`` ABI.
                  Its optional implementation label describes the selected
                  representation without duplicating the schema label.
Type reference    ``NodeTypeRef`` — one-word typed access to the record.
Builder           ``NodeBuilder`` — reusable builder that wraps a
                  type reference and constructs runtime node instances from
                  graph contexts.
Value             ``NodeValue`` — the standalone owning runtime node
                  instance used by focused tests and direct node
                  construction. Graph instances colocate node payloads
                  directly in graph storage and expose them through
                  ``NodeView``.
View              ``NodeView`` — a borrowed type-erased cursor over a
                  node allocation. It carries a two-word ``NodePtr`` (record
                  plus data),
                  pointer, and no cycle-local state. Evaluation time is
                  passed explicitly to lifecycle operations and
                  input/output projection methods. Active input
                  notification targets are recovered from the node
                  runtime storage header.
================  ==========================================================

``NodeBuilder`` is a reusable builder, not a value-builder-style scratch
object. Once the node schema has been resolved to a type record, the builder can
be cached and used to construct many node instances. The instance-specific
inputs are the graph context, node position, boundary bindings, and any
per-node configuration captured by the graph schema.

What a Node Schema Records
--------------------------

A ``NodeTypeMetaData`` carries:

``display_name``
    Optional borrowed display label for diagnostics and graph inspection.

``input_schema``
    Optional. When present, the top-level input schema is a TSB whose fields are
    the named time-series arguments. A single-argument node carries a
    single-field TSB; a multi-argument node carries a multi-field TSB. A source
    node records ``nullptr``. The schema lives in the time-series registry; the
    node schema only holds a borrowed pointer.

``output_schema``
    The time-series schema for the node's primary output (named
    ``output`` at runtime — see *Overview > Node Layer*). It is a
    single TS-shaped schema (``TS<Int>``, ``TSD<Str, TS<Float>>``,
    …) — at most one main output per node. A sink node records
    ``nullptr`` for ``output_schema``. Auxiliary outputs (error,
    recordable state) are recorded as separate properties below, not
    folded into ``output_schema``.

``output_endpoint_schema``
    Endpoint annotation for the primary output. This records whether the output
    endpoint is peered or structural/non-peered and, for structural endpoints,
    the child endpoint layout. It is kept beside ``output_schema`` because
    identical value schemas can have different wiring endpoint topology.

``error_output_schema``
    Optional. The time-series schema for the node's ``error_output``,
    set only for node families that expose an error stream. The default
    callback-backed ops do not install a separate error-handling hook; specialised
    ``NodeOps`` implementations can use ``captures_errors`` and this output
    endpoint together. C++ wiring can select this endpoint explicitly with
    ``error_output(port)``.

``recordable_state_schema``
    Optional. The time-series schema for the node's recordable state — state
    that participates in graph evaluation as a TS rather than as plain scalar
    storage. Used by replay-aware operators. C++ wiring can select this endpoint
    explicitly with ``recordable_state(port)``; it is not the node's ordinary
    output.

``state_schema``
    Optional. The scalar (value-layer) schema for the node's local
    state. Records the field layout and types but no actual values.
    Runtime construction allocates a read-write ``Value`` for this schema,
    default-constructs it through the matching ``StoragePlan`` and
    ``LifecycleOps``, and exposes it through ``NodeView::state``. State
    mutation uses the normal value-layer ``begin_mutation`` path.

``scalar_schema``
    Optional. The scalar configuration bundle for ``Scalar<"name", T>``
    arguments. Scalars are fixed per node instance and are not part of the
    time-series input TSB.

``node_kind``
    Enum: ``Compute``, ``PushSource``, ``PullSource``, ``Sink``, ``Nested``.
    Drives graph execution behaviour and boundary treatment for nested graphs.

``uses_scheduler``
    True when the node requests ``NodeScheduler`` injection. Runtime storage then
    includes a per-node ``NodeSchedulerState`` component.

``schedule_on_start``
    Declarative self-scheduling flag. When true, the default start op schedules
    the node for the graph start cycle after the user ``start`` callback returns.
    Static C++ authoring also enables this for nodes with an active input whose
    schema contains ``REF``: the initial reference binding exists before graph
    start and therefore needs one explicit sample. This is not an input
    modification and is independent of input activation.

``captures_errors``
    Declarative error policy flag for node families that support captured
    exceptions. Allocation of an error endpoint is still represented explicitly
    by ``error_output_schema``.

``active_inputs``
    Optional list of top-level input selector slots that are active. ``nullopt``
    means the runtime default: every top-level input is active. An engaged empty
    vector is an explicit empty selector set: no input tick can activate the node,
    so the node only evaluates when scheduled by some other mechanism.
    Static C++ nodes populate this from ``InputActivity`` flags on ``In``.

``valid_inputs``
    Optional list of top-level input selector slots that must be valid before
    evaluation. ``nullopt`` means the runtime default: every top-level input must
    be valid. An engaged empty vector is an explicit empty selector set: normal
    validity readiness is disabled, and the node body must guard any value reads
    itself. Static C++ nodes populate this from ``InputValidity`` flags on ``In``.

``all_valid_inputs``
    List of top-level input selector slots that must be recursively valid before
    evaluation. This is separate from ``valid_inputs`` so a node can require
    ``all_valid`` for selected structural inputs while other inputs use normal
    validity or are unchecked.

Runtime Value/View
------------------

The runtime node follows the same schema / type-record / plan / ops / owner / pointer
shape as the lower layers:

``NodeTypeRef`` / ``NodePtr``
    ``NodeTypeRef`` is the one-word canonical identity for an interned
    ``(NodeTypeMetaData, Runtime role, StoragePlan, NodeOps)`` record.
    ``NodePtr`` combines that record with node memory for typed runtime access;
    it widens to ``AnyPtr`` for generic diagnostics without another allocation.
    The record's semantic label comes from ``NodeTypeMetaData::header``; its
    implementation label is supplied by ``NodeTypeDescriptor`` or a static
    implementation's optional ``implementation_label`` member. Builder-derived
    variants preserve that representation label.

``NodeValue``
    Owns one node allocation. Moving a node value transfers ownership of
    the allocation; copying is not part of the runtime contract. The node
    storage plan is a composite slab. The
    fixed ``NodeRuntimeStorage`` header is the first component and contains
    only common wrapper state such as graph attachment, node index, lifecycle
    flags, label, and the stable ``Notifiable`` implementation used by active
    input subscriptions. Optional components such as ``TSInput``, ``TSOutput``,
    local ``Value`` state, error output, and recordable-state output are
    present only when declared by the node schema and are laid out after the
    header in the same allocation.

``NodeView``
    Borrowed, type-erased access to a node. Public node operations such as
    ``start``, ``evaluate``, ``stop``, ``input``, ``output``, ``state``,
    and ``cleanup_delta`` dispatch through ``NodeOps``. This keeps graph
    evaluation independent of concrete node implementation classes. The
    state view is writable-capable when the node declares ``state_schema``;
    callers still open mutation explicitly with ``ValueView::begin_mutation``.
    ``NodeView`` wraps ``NodePtr`` and is itself timeless: callers pass ``DateTime`` to
    lifecycle operations and to time-series endpoint projections such as
    ``input(evaluation_time)`` and ``output(evaluation_time)``.

``NodeBuilder``
    Cached construction recipe. It resolves the node schema and endpoint
    annotations to a canonical ``NodeTypeRef``, then constructs ``NodeValue``
    instances on demand. Static implementations and lifted stateless callables
    reuse an existing runtime type when their process-stable implementation
    identity and complete runtime schema match. Derived bindings such as passive
    inputs and error capture are canonicalised independently, so a changed
    policy never aliases the original type.

    Callback-backed extensions use ``NodeBuilder::from_descriptor`` by default;
    each call receives a fresh runtime type because its callbacks may capture
    builder-specific state. An extension may instead call
    ``NodeBuilder::from_canonical_descriptor`` with a non-null token whose
    address remains allocated and is not reused until the node runtime registry
    is cleared. A process-lifetime token is the normal choice. Every descriptor
    using that token must have interchangeable callbacks and ops. Per-node
    values belong in builder scalars or runtime storage, not in a canonical
    callback capture.

    ``visit_child_graphs`` is the cold-path inspection surface for immediate
    compiled child graph templates.  It dispatches through the passive
    ``ChildGraphInspectionOps`` contract, so an extension can recurse through
    nested plans without identifying a concrete map, mesh, reduce, switch or
    single-nested representation.  Ordinary nodes bind the canonical no-op
    implementation.  Each borrowed ``ChildGraphInspectionView`` contains the
    child ``GraphBuilder`` and its optional exposed-output binding.  Its
    pointers are valid only during the visitor call, and evaluation never
    consults this operation.

    Graph runtime types are likewise reused only when the graph label, ordered
    node runtime types, edge paths, and push-source boundary all match. Executor
    runtime types are reused by mode and label. Node labels, scalar values,
    endpoint bindings, graph state, clocks, and executor options remain
    instance-owned and are reconstructed for every run.

The C++ runtime supplies callback-backed and specialised ``NodeOps``
implementations through this structure. Python-authored compute, sink, and
generator nodes use statically compiled C++ trampolines whose records carry
``hgraph.python.*`` implementation labels. They remain ordinary Node/Runtime
records and do not introduce a Python-specific family, role, pointer, or graph
loop branch.

The Node Lifecycle
------------------

A node's runtime behaviour is exposed through the ``NodeOps`` vtable.
The hooks are intentionally minimal:

``start(node*, evaluation_time)``
    Called once per node, after construction and before the first
    ``eval``. Used for any node-local resource acquisition that cannot
    be expressed as plain construction.

``eval(node*, evaluation_time)``
    Called whenever the node is scheduled. The node reads its inputs
    via the bound input views, computes, and writes its output through
    the bound output view. ``eval`` is the only op every node must
    implement.

``stop(node*, evaluation_time)``
    Called once per node, after the last ``eval`` and before
    destruction. Symmetric counterpart to ``start``: releases anything
    ``start`` acquired. Most compute nodes do not need it.

``cleanup_delta(node*)``
    Called after a cycle to clear output, error-output, and recordable-state
    deltas. Input deltas are owned by the upstream time-series endpoints and are
    cleaned through the graph's normal endpoint cleanup.

The first parameter of every op is a pointer to the node's runtime
memory — the same pattern the value-layer ops table uses. This keeps
nodes type-erased: the runtime evaluation loop dispatches against the
ops vtable without knowing the concrete C++ type behind it.

Endpoint and storage projection ops (``input_view_impl``, ``output_view_impl``,
``state_view_impl``, ``scalars_view_impl``, ``scheduler_state_impl``,
``error_output_view_impl``, and ``recordable_state_view_impl``) are also part of
``NodeOps``. They give ``NodeView`` a common interface over native static nodes,
nested nodes, Python-backed nodes, and specialised node families.

Construction and destruction of a node's runtime memory are not part
of ``NodeOps``: they are the responsibility of the bound ``StoragePlan``'s
``LifecycleOps`` (``construct`` / ``destroy``), invoked by the
allocator that owns the node's storage. Keeping them on the plan
preserves the plan/ops separation described in *Allocation, Plans
and Ops*: ``NodeOps`` describes runtime behaviour over already-
constructed memory, ``LifecycleOps`` describes how that memory is
brought into and out of existence.

Generic Resolution
------------------

A node schema may carry **type variables** (``TsVar`` / ``ScalarVar``) —
placeholder schemas that must be resolved at wiring time before the node
can be instantiated. For example, a generic ``add`` node has inputs
``In<"lhs", TsVar<"T">>`` / ``In<"rhs", TsVar<"T">>`` and output
``Out<TsVar<"T">>``; ``T`` is unresolved on the abstract schema and is
bound to a concrete time-series schema at the point the node is wired.

Resolution is **implemented at the wiring layer**
(``include/hgraph/types/type_resolution.h``):

- a ``ResolutionMap`` binds each variable name to concrete metadata;
- ``wire<>`` populates it — ``ts_unifier`` matches each input selector's
  pattern against the connected port's runtime schema (binding input
  variables), a scalar variable is inferred from the configured value's
  type, and a source-side output variable is supplied explicitly
  (``ts_type<>()`` / an explicit output schema);
- ``StaticNodeSignature::is_generic()`` plus its resolution-map schema
  overloads, driven through ``NodeBuilder::implementation<T>(const
  ResolutionMap&)``, then build the node's concrete ``NodeTypeMetaData``.

The node's *callbacks* never see ``T`` — they read the resolved endpoints
from the ``NodeView`` at eval time — so one closure over ``&eval`` serves
every resolution. Different resolutions build distinct concrete
``NodeTypeMetaData`` instances before record interning; any sharing happens through the
normal ``TypeRecord`` key described below, not through
an unresolved generic schema stored on the node metadata. This is the C++
counterpart of Python type-variable resolution and the seed for generic
``map_`` / ``reduce`` / ``switch_``.

Interning Key
-------------

Node types use the common ``TypeRecordRegistry``. The intern key is:

- the ``SchemaHeader *`` at offset zero in ``NodeTypeMetaData``,
- ``TypeRole::Runtime``,
- ``StoragePlan *``,
- ``NodeOps *``,
- optional debug metadata.

The default ``NodeRuntimeRegistry`` owns stable schema, context, and ops storage,
fills missing default ops, builds the storage plan, and then interns the common
record for that concrete key. ``NodeTypeMetaData::header`` supplies the common
family, kind, and semantic label; ``node_kind`` remains its typed mirror and is
validated against the header during interning. The registry does not currently deduplicate ``NodeTypeMetaData``
structurally by field value; schema identity is the stable pointer owned by the
registry.

Status
------

The node-schema vocabulary separates compile-time C++ descriptors from runtime
schema metadata. The compile-time layer is a *generator* that produces concrete
``NodeTypeMetaData`` plus callbacks/ops; the runtime evaluation path consults the
bound schema and ops directly. Generic resolution happens before node
construction through ``ResolutionMap`` and the resolved schema overloads on
``StaticNodeSignature``; unresolved type variables are not stored on
``NodeTypeMetaData``.
