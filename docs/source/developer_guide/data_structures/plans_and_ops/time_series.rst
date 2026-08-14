Time-Series Plans and Ops
=========================

The time-series layer owns full runtime ``TSOutput`` and ``TSInput``
endpoints. A time-series endpoint is not just a scalar ``Value`` with a
timestamp attached: it is split into separate components so the hot
value payload, delta payload, and graph-evaluation state can be stored
and accessed in the shape each one needs.

This page focuses on the layout strategies — memory stability, the
slot-store family, the storage shapes for TSS and TSD, and buffer
exposure. The per-kind tick contract and the value/delta schema
mappings are described in *Schemas > Time-Series Schemas*; the
binding/redirect machinery is described in *Linking Strategies*.

Terminology: TSOutput, TSInput, and TSData
------------------------------------------

The implementation uses the following names consistently:

``TSOutput``
    The output-side top-level time-series holder owned by a node output.
    It owns the root ``TSData`` payload/delta component and output-local
    endpoint state such as dirty cleanup coordination, modification
    publication conveniences, and root-level subscription forwarding.
    ``TSOutput`` is the terminal parent of its root ``TSData`` bubble-up
    chain.

``TSInput``
    The input-side top-level time-series holder owned by a node input. It
    owns an input-specific ``TSData`` root, TargetLink terminal storage,
    activation, deduplication, and scheduling state. Input views usually
    resolve their visible payload through a bound output rather than
    owning an independent copy of the output data.

``TSState``
    The graph-evaluation state associated with a time-series endpoint:
    validity, ``last_modified_time``, parent/child relationships,
    subscribers, path identity, and kind-specific notification state.
    ``TSState`` does not own the hot payload bytes.

    .. note::

       ``TSState`` is a *conceptual grouping*, not a distinct runtime type in
       the current code. The state it names is realised inside ``TSData`` —
       chiefly the per-level ``TSDataTracking`` record (``last_modified_time``,
       ``TSDataParentLink``) and ``TSDataObserverSet`` — together with
       endpoint-local state on ``TSOutput`` / ``TSInput``. Use the name when
       discussing the *role*; do not expect a ``TSState`` class to exist.

``TSData``
    The payload/delta component owned by ``TSOutput`` and projected by
    ``TSInput``. Real output TSData owns the current value storage and
    per-tick delta information, laid out so those two views stay aligned
    and can expose useful buffer / NumPy representations. Non-peered
    input prefixes expose a TSData-shaped projection backed by
    input-local state and child target links rather than by a copied
    aggregate payload. ``TSData`` also owns or projects the per-level
    observer set for modification notification and the local parent link
    used for bubble-up. It does not own graph scheduling policy.

``TSDataOps``
    The type-erased operation table over a ``TSData`` memory region:
    the literal ``allows_mutation`` property, common layout access,
    current-value validity, recursive ``all_valid`` checks, read/write
    memory access, delta reset, copy/move value assignment, canonical delta construction,
    erased delta capture/apply, and the per-kind hook used when a child
    time-series value reports that it modified. Implementations that own
    nested TSData also publish a private ``ownership_ops`` lifecycle projection;
    this is distinct from the visible indexed or keyed view projection. The
    table is deliberately passive; generic mutation sequencing and propagation
    rules live on ``TSDataView`` / ``TSDataMutationView``. For a real bound ``TSData``
    implementation the table is total: required entries are never null,
    empty optional behaviours use no-op thunks, and unsupported
    operations are explicit throwing thunks rather than missing
    pointers.

``TSDataObserverSet``
    The compact observer set stored in each ``TSDataTracking`` record.
    Empty levels store a null tagged pointer, a single observer stores the
    observer pointer directly, and the second observer promotes the set to
    a heap-owned vector of observer pointers. This keeps unsubscribed
    levels at one pointer of overhead while still allowing multiple
    bindings at the same time-series level. Removing an observer during
    notification marks a tombstone in the active vector and compacts after
    the outermost notification pass completes; normal removal outside
    notification remains swap-with-back and pop. Observers added during a
    notification pass are appended but are not notified for the already
    in-flight modification. Producer invalidation is a distinct detached
    traversal: the set becomes empty before callbacks run, callbacks receive
    ``source_invalidated``, and the detached storage is reclaimed after the
    outermost pass. This path performs no allocation and observers must not
    use ordinary unsubscribe against the invalidating source.

``TSDataLayout``
    The common layout prefix for every TSData kind. It records only the
    offsets/bindings required at the "this is a time-series payload"
    layer: current value, delta value, and local tracking. It does not
    describe every possible collection shape.

``FixedTSBDataLayout`` / ``FixedTSLDataLayout``
    Specialised layouts for fixed structured TSData. ``TSB`` keeps
    per-field entries because each field can have its own schema and
    offset. Fixed ``TSL`` keeps the element count and value/auxiliary
    strides because every element has the same schema and fixed span.
    Other major TSData families use their own specialised layouts rather
    than adding more shape fields to ``TSDataLayout``.

``TSWDataLayout`` / ``SizeTSWDataLayout`` / ``TimeTSWDataLayout``
    Specialised layouts for window TSData. ``TSWDataLayout`` is only
    the common window prefix: payload element binding, timestamp
    element binding, plus the normal value, delta, and tracking offsets
    inherited from ``TSDataLayout``.
    ``SizeTSWDataLayout`` carries ``period`` and ``min_period`` for
    tick-count windows. ``TimeTSWDataLayout`` carries ``time_range`` and
    ``min_time_range`` for duration windows. A concrete ``TSW`` schema
    resolves to exactly one of those layouts; it never switches between
    the two models at runtime.

``IndexedTSDataOps``
    The shared view-facing indexed access surface for TSData
    shapes. ``TSB`` and ``TSL`` can expose common indexed operations
    such as ``size()``, ``at(index)``, and value/item iteration while
    still using different concrete storage ops. Fixed ``TSL`` and
    dynamic ``TSL`` therefore share ``TSLDataView`` but do not have to
    share the same layout or mutation implementation.

``TSRoleTypeRef`` / ``TSDataTypeRef`` / ``TSInputTypeRef`` / ``TSOutputTypeRef``
    One-word references to canonical ``TypeRecord`` instances. The generic
    role reference preserves the runtime role; the three checked wrappers
    require ``Data``, ``Input``, or ``Output`` respectively. They cover
    for scalar ``TS<T>`` / ``SIGNAL``, fixed ``TSB`` / fixed ``TSL``,
    keyed ``TSS`` / ``TSD``, dynamic ``TSL``, ``TSW``, and ``REF`` roots and
    descendants. The records
    share the time-series schema but have distinct ``Data``, ``Input``, and
    ``Output`` roles. Data and Output select mutable role-specific ops; an
    owned Input selects the corresponding physical plan under a read-only
    role, while peered positions select target-link storage and ops.
    ``TS_DATA_OPS_ABI_VERSION`` is 9. ABI 9 adds an explicit data-only
    inspection table for debugger-visible representation fields. ABI 8 adds
    recursive source-topology validation to the current-state strategy table
    introduced by ABI 7. ABI 6
    added the erased Python-conversion operations. ABI 5 added the cold-path
    dynamic-storage attribution hook used by GraphDiagnostics. ABI 4 represented
    destructive value assignment sources as writable ``ValueView`` pointers
    rather than owning ``Value&&`` objects.

``TSDataStorageRef<DataOps>``
    The non-owning time-series cursor. Every generic and specialised form is
    exactly two words: a ``TSRoleTypeRef`` and a data pointer. Ops, schema, and
    plan are reached through the canonical record rather than cached in the
    cursor. A typed-null cursor may retain its type record while carrying no
    data; it is not live and its generic ops surface reports the default empty
    behaviour. Specialised cursors validate their TS kind when constructed.

``TSDataPlanFactory``
    The schema → data-plan resolver for ``TSData``. It chooses the
    compact mutable implementation for atomic time-series data, fixed
    structured plans for ``TSB`` / fixed ``TSL``, window plans for
    ``TSW``, and slot-oriented plans for keyed or dynamically-sized
    collection-shaped time-series data. The factory does not plan the
    whole endpoint object, only its payload/delta data component.

``TSOutputBuilder`` / ``TSInputBuilder``
    Reusable builders for top-level time-series endpoints. An
    ``TSOutputBuilder`` composes an Output role record and its data plan with
    output endpoint state. A
    ``TSInputBuilder`` consumes an endpoint
    annotation tree compiled by ``TSInputPlanFactory`` and builds input
    endpoint storage: non-peered TSB/fixed-TSL input TSData prefixes,
    TargetLink terminal storage, activation state, and scheduling hooks.
    It does not allocate an independent output-payload copy for the
    visible input value.
    These builders are cached by node and graph construction code.

    Migrated roots support peered and, where the schema has an owning plan,
    owned input annotations. Direct peered roots own only target-link storage
    and preserve bind/unbind/rebind plus active/passive subscription semantics.
    Owned inputs use read-only Input-role records over their physical storage.
    A non-peered ``TSD`` input may also be a composite structural prefix whose
    element topology is compiled independently.

``TSEndpointSchema``
    The generic annotation layer over a canonical
    ``TSValueTypeMetaData`` schema. Each annotation node says whether
    that time-series position is ``non_peered`` or ``peered``. This
    layer is not input-specific; inputs compile peered terminals into
    target-link binding state, and REF/link infrastructure can reuse it
    for the same peered vs non-peered distinction.

TSData implementation families
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``CompactTSDataStorage``
    Used for atomic time-series data. The current payload uses the
    compact scalar ``StoragePlan`` for the value type, but the bound
    ``TSDataView`` allows mutation because a time-series output updates
    in place during evaluation. Current value bytes and tracking stamps
    are separate memory regions in one TSData plan. The atomic delta
    view aliases the current value when ``last_modified_time`` matches
    the evaluation time, so no duplicate scalar delta payload is
    allocated.

``FixedStructuredTSDataStorage``
    Used for fixed-shape structured time-series data: ``TSB`` and
    fixed-size ``TSL``. The parent plan allocates the complete current
    value as one canonical value-layer region, followed by an auxiliary
    tree containing child and parent tracking. The full memory footprint
    of the parent and all fixed children is known before allocation, and
    child views use role-specific ``TSRoleTypeRef`` records whose ops carry
    offsets into the shared root value and auxiliary regions. Dynamic-list and
    window children use their canonical embedded role records over independent
    child plans.

``WindowTSDataStorage``
    Used for ``TSW``. It exposes one common ``TSWDataView`` surface but
    has two concrete storage models: a fixed-capacity cyclic buffer for
    tick-count windows and a timestamped queue for duration windows.
    ``SizeTSWindowStorage`` and ``TimeTSWindowStorage`` share only the
    low-level timestamp/payload buffer management; push, pruning, and
    capacity policy are selected by the concrete storage type. Both
    models store two aligned value-element buffers: a ``DateTime``
    timestamp buffer and a payload ``T`` buffer. The current-value
    surface is list-shaped over the payload buffer, but the bound value
    ops project directly over the window storage instead of
    materialising a compact immutable value-layer ``ListStorage``.

    Validity is minimum-gated for BOTH models: a tick window is TS-valid
    only once it holds ``min_period`` elements, and a duration window
    only once its span reaches ``min_time_range`` (with the unset
    minimum defaulting to the full period — hgraph's rule). The delta
    stream flows pre-validity for tick windows (the harness records
    those ticks); duration windows stay silent below their span. Each
    push also stashes the element it evicts (a full tick window rolling,
    or the span drop) with its eviction time. Data-level
    ``has_removed_value(evaluation_time)`` and
    ``removed_value(evaluation_time)`` require the queried cycle explicitly;
    input endpoint and Python ``TimeSeries`` conveniences supply their current
    evaluation time. A later no-tick cycle cannot observe retained eviction
    storage.

    Dynamic ``TSL`` and both window models use distinct canonical ABI-2
    records for Data, Input, and Output roles while sharing one physical plan
    per schema. Root and embedded records remain distinct. Their implementation
    labels are ``ts.tsl.dynamic.{data,output}.root``,
    ``ts.tsl.dynamic.input.owned``,
    ``ts.tsl.dynamic.{data,input,output}.embedded``, and the corresponding
    ``ts.tsw.{tick,duration}.*`` forms.

``SlotTSDataStorage``
    Used for keyed collection time-series data such as ``TSS`` and ``TSD``.
    The data store is slot-oriented:
    every child or element has a stable slot id and the current payload,
    validity, and delta information are aligned by that slot id.
    Collection mutation changes slot state instead of compacting or
    relocating already-published child addresses.

    Root, embedded, key-set, and TSD value projections have distinct canonical
    role records. In particular the projections are named
    ``ts.tsd.key-set.{data,input,output}`` and
    ``ts.tsd.value.{data,input,output}``, so a debugger can identify topology
    without inferring it from an ops pointer. The implementation label is part
    of canonical record identity, so these projections retain the exact opaque
    ops table selected for the element realization; no derived-table copy or
    kind-based narrowing is required.

    Slot lifetime follows the observer protocol. Removing a key stops its
    owned child tree and marks the slot not live, but retains the constructed
    key and child for the rest of the engine cycle. Reinsertion during that
    interval resurrects the same slot without allocating or reconstructing its
    payload. The later erase notification precedes child and key destruction;
    only then may the slot be reused.

``DynamicTSLStorage``
    Used for ``TSL<C, 0>``. It is indexed, homogeneous child TSData
    storage rather than key/slot storage: each element owns a stable
    child ``TSData`` handle and vector growth moves only those handles,
    not the child TSData allocations they point at. The current value is
    projected as a dynamic value-layer ``List`` over child current values
    and the delta is projected as ``Map<int, delta(C)>`` over children
    modified with the parent. Because that delta schema has no removal
    surface, dynamic ``TSL`` TSData is currently grow-only; copying a
    shorter list is rejected. The role-neutral storage binds its one-word
    element ``TSRoleTypeRef`` on first growth and rejects a later type
    mismatch. Destruction walks owned children and invalidates observers before
    destroying their record-backed storage handles; there is no retired-child
    side channel.

The terms above keep three layers distinct: scalar ``Value`` storage,
``TSData`` payload/delta storage, and top-level ``TSOutput`` /
``TSInput`` endpoint state. Code and docs should avoid using "TS value
plan"; payload/delta plans are ``TSData`` plans, and endpoint plans are
output/input plans.

TSInput Construction and Scheduling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The top-level input endpoint is always a non-peered ``TSB``. This TSB
represents the node input bundle and exists even when a node has a
single logical input. The node's input schema is known at wiring time,
then augmented with a ``TSEndpointSchema`` annotation tree. That tree
records which schema positions are structural, non-peered prefixes and
where traversal steps into a peered terminal.

For example, for this schema:

.. code-block:: text

   TSB[{ts: TSL[TS[int], 2]}]

the annotation can bind the whole list from one target:

.. code-block:: text

   TSB[non_peered, {ts: peered[TSL[TS[int], 2]]}]

or make the list itself non-peered and bind each element separately:

.. code-block:: text

   TSB[non_peered, {ts: TSL[non_peered, peered[TS[int]], 2]}]

The explicit child form is useful for fixed ``TSL`` because the element
schema is homogeneous but the endpoint topology can still differ by
index. For example, for ``TSL[TSB[{x: TS[int]}], 2]`` the first index
can be peered as one whole bundle while the second index remains a
non-peered bundle whose field is peered independently:

.. code-block:: text

   TSL[
     non_peered,
     [
       peered[TSB[{x: TS[int]}]],
       TSB[non_peered, {x: peered[TS[int]]}]
     ],
     2
   ]

``TSEndpointSchema::non_peered_list`` is the shorthand for the common
case where every fixed-list index has the same annotation.

``TSInputPlanFactory`` validates the annotation against the schema and
compiles the input construction plan. For TSInput, the root must be a
non-peered ``TSB``. Under that root the current implementation permits
one or more non-peered ``TSB`` / fixed-size ``TSL`` prefixes, and a
non-peered ``TSD`` composite prefix, followed by a ``peered`` terminal.
Owned keyed or reference roots are also valid where the annotation selects
owned storage. Once traversal reaches ``peered``, the
subtree from that point downward is associated with one output peering;
the TSInput implementation represents that peering with TargetLink
storage inside the input data plan.

Non-peered dynamic ``TSL`` prefixes are still not supported because
input-side path identity for unbounded indices needs an explicit
structural policy. Peered terminals may still bind to collection
outputs; once bound, input navigation inside that target uses the
output's ``TSData`` internally while still returning input-shaped
endpoint views from the public API. For a target-bound dynamic ``TSL``,
``TSLInputView::size()`` reports the target output's live list size.

.. mermaid::

   flowchart TD
      Root["TSInput root<br/>non-peered TSB"]
      Prefix["planned input TSData prefix<br/>TSB / fixed TSL / TSD"]
      Link["peered terminal<br/>TargetLink storage"]
      Output["TSOutputView"]
      Data["output-owned TSData"]

      Root --> Prefix
      Prefix --> Link
      Link -->|bind_output| Output
      Output --> Data

The input endpoint owns planned input TSData and scheduling state, but
does not copy output payloads into non-peered prefixes. Binding a target
link registers an internal target observer on the bound ``TSData``
level. That observer updates the peered input terminal's
``last_modified_time`` and bubbles the modification through any
non-peered parents. Active input views install scheduling notifiers on
the input TSData level they activate, or directly on a descendant output
``TSDataView`` when activation navigates inside a bound target
collection. The final scheduling target is a ``Notifiable`` supplied by
the owning runtime node.

TargetLink in-plan storage is deliberately small: the ordinary
``TSDataTracking`` record plus inline target-link state. The common plan owns
a borrowed ``TSOutputHandle`` containing the output identity and output-owned
``TSDataView``, the target-modified observer, the scheduling notifier, an
optional active descendant trie rooted at the peered boundary, and a pointer
to the immutable structural ops table. The state is inline because the normal
runtime case is a bound link; only the sparse descendant trie nodes are
allocated on demand.

Wiring selects a larger concrete plan only for ``TSS`` and ``TSD`` peered
terminals. That implementation-only representation adds a one-word compact
slot-observer list and a lazy structural-transition allocation. All other
terminal kinds use the canonical no-op structural table and pay for neither
field. Raw storage access is selected alongside the plan, so the rest of the
input endpoint machinery continues to consume the common TargetLink contract
without naming the structural representation.

Views are not stored in TargetLink state. Endpoint views materialize
transient ``TSDataView`` cursors from the stored ``TSOutputHandle`` when
they need to navigate or read the target. They also avoid storing
input and target path vectors. Non-peered positions derive their
logical input path from ``TSDataParentLink`` only when activation needs
to touch the root active trie. Positions below a TargetLink carry a
pointer into the link's descendant trie; that node records the
input-to-output transition identity, while the projected output
``TSDataView`` keeps the output-owned parent path.

.. mermaid::

   sequenceDiagram
      participant Output as Output TSData
      participant TLink as TargetLink observer
      participant Prefix as Non-peered parents
      participant Active as Active scheduling notifier
      participant Node as Owning node Notifiable

      Output->>TLink: notify(modified_time)
      TLink->>TLink: record local modified time
      TLink->>Prefix: bubble child modified
      Prefix->>Active: notify(modified_time)
      Active->>Node: notify(modified_time)

The scheduling notifier is separate from the target observer. The
target observer keeps input-local modification state aligned with the
bound output. The scheduling notifier is installed through the active
trie only for active input paths and forwards to the owning node. The
trie records boundary-relative path identity without storing an eager
map or vector-valued path on every input view.

Destroying an output or replacing its root storage invalidates published
target links before that storage is destroyed for every record-backed root,
including dynamic ``TSL`` and ``TSW``. The link state is the producer-side
registration token:
its callback never dereferences the dying producer. It first drops every
borrowed output handle and marks output-side slot subscriptions absent, then
notifies local slot observers after the borrowed state is empty. Input-side
active topology remains in place and producer teardown does not schedule the
node, so a later bind reuses the existing active/passive state. Output copy
and move operations do not transfer published bindings: assignment
invalidates the destination, and moving also invalidates bindings to the
source before moving its value. Locally owned fixed TSB/fixed-TSL descendants,
composed prefixes, dynamic-list descendants, and their local TargetLink
terminals are invalidated in pre-order before their storage is destroyed.
Keyed descendants retain their delayed delete/erase lifetime through the slot
observer protocol; a window has no owned time-series descendants to traverse.

Lifecycle traversal deliberately differs from view-facing indexed traversal.
Each concrete implementation publishes its private, static ownership projection
directly through ``TSDataOps::ownership_ops``: TargetLink contexts are leaves;
composed input/output contexts return
their cached local child storage types and in-plan storage addresses; regular
fixed contexts return their cached fixed child types and local absolute
addresses. Dynamic lists expose their owned child handles to this traversal;
windows and TargetLinks are leaves. Keyed shapes coordinate child destruction
through their slot stores. No function address or schema kind is used as a
runtime implementation identifier. Consequently attach, reparent,
invalidation, and auxiliary-memory accounting cannot follow a visible
TargetLink projection into producer-owned storage. The ownership table reports
TargetLink trie/observer storage at the owning endpoint and traverses only
owned children. This projection is private lifecycle infrastructure; it adds no
storage-layout cost, and its ops-table ABI contribution is tracked by
``TS_DATA_OPS_ABI_VERSION``, currently 9.

Fixed to-REF alternatives are the exception to the general legacy-alternative
rule. Their allocation is owned through the canonical Data-role record. At the
``TSOutputHandle`` boundary the alternative checks that the Output-role record
uses the same plan and layout-compatible ops, then publishes that Output record
over the same allocation. Nested fixed children therefore project Output-role
embedded records while strict checked conversion of a raw Data-role handle to
``TSOutputTypeRef`` still fails. Migrated composed from-REF roots use their
Output-role composed type directly; excluded alternative families remain
binding-backed ownership leaves.

Active target-link inputs also notify on sampled live bind operations. Binding
or rebinding an active input to an already-valid source schedules the owning
node at the current evaluation time, even if the source did not modify during
that cycle. Rebinding an existing active target schedules for the same reason:
the sampled source identity changed. Scalar and fixed-shape REF unbinds remain
silent; keyed ``TSS``/``TSD`` unbinds schedule only when they reconcile a
previously published key set. This is the sampled-input mechanism used by
nested operators such as ``switch_``; it writes the graph schedule table
through the normal notification path rather than bypassing node evaluation.
See :doc:`../../binding_vocabulary` for the terminology.

Non-peered input prefixes are structural TSData projections with Input-role
type records and value projections. They do not copy the bound output's
aggregate payload; instead, their input-local erased ops project the
current value and child TSData views from their children. This keeps the
public surface consistent with output views while avoiding duplicate
payload storage for non-peered prefixes.

These projected value records borrow the TSInput root allocation only while
they remain views. Constructing or cloning a ``Value`` materialises through
``ValuePlanFactory::type_for(projected.schema())``; the resulting value owns
canonical storage and may outlive the input. TSB current/delta projections,
nested bundles, map deltas, and key-set adapters preserve unset entries when
their canonical schema carries validity state.

A fixed TSL has a deliberate boundary distinction. Its input-side projected
view reports an invalid child as a typed-null element (and the direct Python
value path reports that child as ``None``). Its canonical owning schema is the
dense fixed ``List[E, N]``, so owning construction copies live children and
default-fills invalid positions with canonical ``E`` defaults. The TSL delta
map continues to represent absence by omitting the ordinal key. If the list or
element plan cannot default-construct that fill value, owning construction
throws; it does not fabricate bytes or invent a nullable fixed-list layout.

Their state is recorded on the input prefix and updated by the same
child-modified bubble-up path used for scheduling:

- ``type_ref()`` returns the prefix's Input-role record even before any child
  target is bound; migrated fixed prefixes return ``nullptr`` from
  ``binding()``;
- ``valid()`` is true when any child is valid;
- ``all_valid()`` is true only when every child is valid;
- ``modified()`` is true when the prefix was marked at the view's
  evaluation time;
- ``last_modified_time()`` is the prefix's bubbled modification time, or
  ``MIN_DT`` when no child has modified it.

Binding state is part of the public input contract, but it is only a
meaningful target-link predicate for views where ``is_bindable()`` is
true. ``is_bindable()`` reports whether the view is backed by a peered
TargetLink whose target can be bound or rebound. This includes child
views projected inside an already-bound peered target; calling
``bind_output()`` / ``unbind_output()`` there operates on the enclosing
TargetLink and then reprojects the child path. For bindable
peered views, ``bound()`` reports whether an output target is currently
linked. For non-bindable views, including non-peered prefixes,
``bound()`` is always true: those views own input-local TSData/value
projection state, and ``bound()`` deliberately avoids recursively
scanning descendants. Invalid or unbound peered endpoints still retain
their typed binding; their current value view is typed-null until a
target is bound and has a current value.

Shape casts on endpoint views stay endpoint-shaped. ``TSInputView``
therefore returns ``TSSInputView``, ``TSDInputView``, ``TSBInputView``,
``TSLInputView``, and ``TSWInputView`` from ``as_set()``,
``as_dict()``, ``as_bundle()``, ``as_list()``, and ``as_window()``.
These specialised input views expose the normal input behaviours such
as ``bind_output()``, ``unbind_output()``, ``bound()``,
``is_bindable()``, ``make_active()``, ``make_passive()``, and
``active()`` at the projected level.

``TSBInputView`` and ``TSLInputView`` expose the structural navigation
expected from the corresponding output/data views: ``values()``,
``valid_values()``, ``modified_values()``, ``items()``,
``valid_items()``, and ``modified_items()``. Bundle input views also
expose ``keys()`` and name-based field access. ``TSDInputView`` uses
keyed lookup while returning child ``TSInputView`` instances for
dictionary values. These helpers return ``Range`` or ``KeyValueRange``
surfaces, matching the value and TSData APIs; callers iterate them
instead of receiving materialised vectors. The specialised input views
also expose ``data_view()`` for the underlying TSData-shaped projection
and ``value()`` for the value-layer view.

``TSOutputView`` follows the same rule. Its shape casts return
``TSSOutputView``, ``TSDOutputView``, ``TSBOutputView``,
``TSLOutputView``, and ``TSWOutputView`` rather than raw
``TSDataView`` specialisations. Output-specialised child navigation
returns child ``TSOutputView`` instances so mutation and observer
operations remain available at the projected level. When code needs
the raw TSData surface it uses the specialised endpoint view's
``data_view()`` method explicitly; when it needs the value-layer shape
it uses ``value().as_set()``, ``value().as_map()``,
``value().as_bundle()``, or ``value().as_list()`` as appropriate.
``TSOutputView::bound()`` reports whether the view carries live output
storage, which is distinct from ``valid()``; an output can be
bound before its time-series value has ever been set.

One-level endpoint visitation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generic endpoint algorithms can dispatch a live ``TSInputView`` or
``TSOutputView`` with ``hgraph::visit`` from
``<hgraph/types/time_series/visitor.h>``. The visitor performs one switch on
the endpoint's semantic ``TSTypeKind`` and passes a borrowed specialised view
to the matching callable. Existing collection views are used for ``TSS``,
``TSD``, ``TSL``, ``TSW``, and ``TSB``; ``TSValue*View``,
``TSReference*View``, and ``TSSignal*View`` tag the three leaf kinds.

.. code-block:: cpp

   const std::size_t child_count = hgraph::visit(
       input,
       [](hgraph::TSDInputView dict) { return dict.size(); },
       [](hgraph::TSLInputView list) { return list.size(); },
       [](hgraph::TSBInputView bundle) { return bundle.size(); },
       [](hgraph::TSInputView) { return std::size_t{0}; });

A specialised handler takes precedence over the role-level
``TSInputView`` / ``TSOutputView`` catch-all. Every reachable branch must
return ``void`` or the same safe value type. Reference returns and lazy
``Range`` / ``KeyValueRange`` results are rejected because the selected wrapper
is a temporary cursor and a range may retain it as projection context. Consume
ranges inside the handler or return an owned materialisation such as
``std::vector``. User-defined result types must likewise not retain a pointer or
reference to the selected wrapper.

Dispatch is intentionally one level. It does not walk collection children or
follow ``REF`` values. A caller that wants recursion selects children through
the specialised view and calls ``visit`` again, making key selection,
validity filtering, and reference-cycle policy explicit. An endpoint with a
schema remains dispatchable while its current value or peered target is
invalid; a default view without a schema throws ``std::invalid_argument``.

TSData Memory Layout and Delta Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every ``TSData`` plan makes current-value access and delta-management
rules explicit. For compact atomic ``TS<T>``-style data, the delta
value is the current value and is valid only when
``last_modified_time == evaluation_time``. Collection storage adds
separate delta masks or payloads where the delta shape is not the
current value. The layout context carried by ``TSDataOps`` records the
bindings and memory offsets needed by the concrete implementation:

- ``value`` — the current payload in the value-layer representation;
- ``delta`` — optional per-tick delta payload or masks, using
  ``delta_value_schema`` when the delta is not an alias of the current
  value;
- ``tracking`` — the common ``TSDataTracking`` record. Every TSData
  shape, including compact atomic TSData, has one. It stores
  ``last_modified_time`` and the optional ``TSDataParentLink`` used
  when the data is projected as a child.

The diagrams below are conceptual. ``StoragePlan`` still owns the
exact byte offsets, padding, and alignment decisions for the target
platform. The invariant is the shape and ownership of each region, not
the literal byte numbers shown in a diagram.

Compact Atomic TSData
^^^^^^^^^^^^^^^^^^^^^

``TS<T>``, ``REF<T>``, and ``SIGNAL`` currently use a compact TSData
plan. The owning memory is one storage object with two separately
addressable regions:

.. code-block:: text

   TSData storage allocation
   +---------------------------------------------------------------+
   | value region                                                  |
   | Value storage for T, also used as delta(T) when modified      |
   | layout.value_offset                                           |
   +---------------------------------------------------------------+
   | tracking region                                                |
   | TSDataTracking {                                                |
   |   last_modified_time                                           |
   |   parent link                                                  |
   | }                                                              |
   | layout.tracking_offset                                         |
   +---------------------------------------------------------------+

The current-value read path points directly at the ``value`` region.
The delta read path returns an empty typed view unless
``tracking.last_modified_time == evaluation_time``; when it matches,
the delta view points directly at the current ``value`` region:

.. mermaid::

   flowchart LR
      View["TSDataView"]
      ValueCall["value()"]
      DeltaCall["delta_value(t)"]
      Current["value region<br/>current T bytes"]
      Tracking["tracking region<br/>last_modified_time"]
      Check{"last_modified_time == t"}
      Null["typed null ValueView"]

      View --> ValueCall --> Current
      View --> DeltaCall --> Check
      Tracking -.read.-> Check
      Check -->|yes| Current
      Check -->|no| Null

During mutation, the first write in an evaluation cycle copies the source
payload into the current value region, then updates
``last_modified_time``. A same-time overwrite writes the value region again
but does not advance ``last_modified_time`` and does not produce a
second parent notification:

.. mermaid::

   flowchart TD
      Write["write source at evaluation time t"]
      Check{"already modified at t?"}
      FirstCopy["copy source into<br/>current value"]
      FirstMark["last_modified_time = t<br/>parent notification may bubble"]
      OverwriteCopy["overwrite<br/>current value"]
      NoNotify["last_modified_time unchanged<br/>no second parent notification"]

      Write --> Check
      Check -->|no| FirstCopy --> FirstMark
      Check -->|yes| OverwriteCopy --> NoNotify

.. code-block:: text

   before write at t
   +----------+   +----------------------------+
   | current  |   | last_modified_time = t - 1 |
   +----------+   +----------------------------+

   first write at t
   +----------+   +----------------------------+
   | source   |   | last_modified_time = t     |
   +----------+   +----------------------------+

   overwrite again at t
   +----------+   +----------------------------+
   | source2  |   | last_modified_time = t     |
   +----------+   +----------------------------+

Fixed Structured TSData
^^^^^^^^^^^^^^^^^^^^^^^

``TSB`` and fixed-size ``TSL`` use recursive fixed layouts. The parent
storage plan starts with one canonical value-layer region for the full
current value, followed by an auxiliary tracking tree. This keeps all
current value bytes collected together under the same recursive layout
that the value layer uses. For example ``TSL[TS[int], Size[2]]`` stores
its current values as the exact fixed ``List[int, 2]`` value plan, so
the value region is suitable for buffer-oriented access.

.. code-block:: text

   Fixed structured TSData allocation
   +--------------------------------------------------------------+
   | value region                                                 |
   | canonical ValuePlanFactory plan for TSData.value_schema      |
   | e.g. TSL[TS[int], Size[2]] -> fixed List[int, 2]             |
   +--------------------------------------------------------------+
   | auxiliary region                                             |
   | TSB: field_0/field_1/... auxiliary tracking trees            |
   | TSL: elements auxiliary array with fixed element stride       |
   | parent TSDataTracking { last_modified_time, parent link }     |
   +--------------------------------------------------------------+

For ``TSL[TS[int], Size[2]]`` the current-value part is therefore:

.. code-block:: text

   value region
   +-------------------------+
   | int[0] | int[1]         |
   +-------------------------+
   | stride == sizeof(std::int32_t)   |
   +-------------------------+

and the tracking side is separate:

.. code-block:: text

   auxiliary region
   +--------------------------------------------------------------+
   | elements[0] tracking | elements[1] tracking                  |
   | parent tracking                                              |
   +--------------------------------------------------------------+

The concrete layouts reflect that difference. ``FixedTSBDataLayout``
keeps per-field entries because each field may have a different TSData
schema and offset. ``FixedTSLDataLayout`` stores the element count plus
the current-value and auxiliary strides; element offsets are computed as
``base + index * stride``.

For nested fixed structures the same rule is applied recursively inside
the value region. A ``TSB`` field that is a fixed ``TSL`` points into a
bundle field whose payload is the fixed list value plan; the child and
grandchild TSData views use embedded role records whose ops carry offsets
into the shared value and auxiliary regions.

**Embedded projected children.** A fixed parent may also own children
whose storage is not just an offset into the parent's value region. This
includes slot-oriented ``TSS`` / ``TSD`` children, dynamic-list ``TSL``
children, and window-oriented ``TSW`` children. The child's complete
TSData storage plan is placed as that child's auxiliary node, and the
parent indexed TSData ops return a pointer to that child storage
subobject when the child is selected. The child storage type is the canonical
embedded role record over the independent child plan; slot, dynamic-list, and
window ops still receive a pointer to their own storage object and do not know
about the parent's root allocation. Parent notification uses the existing
``TSDataParentLink`` installed by child view projection.

When a fixed parent contains projected child storage, its ``value()`` and
delta surfaces are projected from child views instead of exposing a stale
canonical value-region copy. Copying those transient views materialises
normal canonical value-layer ``List`` / ``Bundle`` / ``Map`` / ``Set``
storage. Fixed ``TSL`` and ``TSB`` can therefore contain any implemented
non-``REF`` child kind: ``TS``, ``SIGNAL``, ``TSS``, ``TSD``, fixed
and dynamic ``TSL``, ``TSB``, and ``TSW``. Dynamic ``TSL`` storage is
grow-only: it can add indexed children and project current/delta values,
but it cannot shrink because ``Map<int, delta(C)>`` has no removal
surface.

.. mermaid::

   flowchart LR
      Root["root TSData allocation"]
      Value["value region<br/>Bundle/List value plan"]
      Aux["auxiliary region<br/>child tracking trees + parent tracking"]
      ChildView["child TSDataView<br/>data = root base<br/>record ops select child value/tracking"]

      Root --> Value
      Root --> Aux
      ChildView --> Value
      ChildView --> Aux

Projected child storage objects are therefore prepared and
default-constructed as part of parent construction. Dynamic ``TSL``
elements are the exception: the dynamic list storage object is
constructed with the parent, but indexed child TSData elements are
allocated lazily as the list grows.

The parent ``value()`` view is the canonical value-layer view over the
value region. ``TSB.value()`` exposes the bundle binding for the full
current value. Fixed ``TSL.value()`` exposes the fixed list binding for
the full current value. Fixed elements are reached through specialised
views using the standard time-series API names:
``as_bundle()`` for ``TSB`` and ``as_list()`` for ``TSL``. Generic
``TSDataView`` does not expose indexed traversal. Fixed and dynamic
``TSL`` may use different data ops and layouts, but callers use the
same ``TSLDataView`` surface for indexed list semantics.
``TSLDataView`` exposes ``values()``, ``valid_values()``,
``modified_values(evaluation_time)``, ``items()``, ``valid_items()``,
and ``modified_items(evaluation_time)``. ``TSBDataView`` exposes the
same positional operations plus ``keys()``, string-keyed ``items()``,
``valid_items()``, and ``modified_items(evaluation_time)``.

.. code-block:: text

   fixed TSL current value memory
   +--------------------------------------------------------------+
   | root.value()                                                 |
   | ValueView{List[int, 2], value_offset}                        |
   +--------------------------------------------------------------+
   | as_list().at(0).value() -> element 0                         |
   | as_list().at(1).value() -> element 1                         |
   +--------------------------------------------------------------+

The parent ``delta_value(t)`` view is valid when the parent
``last_modified_time == t``. For ``TSB`` it exposes a bundle-shaped
delta where each field projects to the matching child's delta if that
child also modified at ``t``; unmodified fields are typed-null. For
fixed ``TSL`` it exposes the documented map-shaped delta
``Map<int, child.delta>`` and iterates only child indices modified at
``t``.

Projecting a child stores a ``TSDataParentLink`` in the child node's
tracking region. The link records the immediate parent storage-type/data
identity (a canonical role record) and the parent-local field/index
id. It does not point at the
parent view object. When a child modification is first recorded in an
evaluation cycle, the child bubbles that id to the parent; the parent then
records its own ``last_modified_time`` for the same evaluation time and
continues through its own tracking link if it has one. The root
``TSData`` link terminates at the owning endpoint, for example
``TSOutput``, which records endpoint-local dirty state instead of
recording another TSData child id.

Window TSData
^^^^^^^^^^^^^

``TSW<T>`` stores the current rolling window and exposes a scalar delta:
the element pushed at the current evaluation time. The value schema is
still list-shaped:

- tick-count windows expose ``List<T, period>``;
- duration windows expose ``List<T, 0>`` because the number of elements
  in the time range depends on tick rate.

The TSData plan has a window storage component plus the common tracking
stamp. There is no separate delta region. ``delta_value(t)`` returns the
latest element in the window when ``last_modified_time == t``; otherwise
it returns a typed-null scalar view.

.. code-block:: text

   TSW TSData allocation
   +--------------------------------------------------------------+
   | window region                                                |
   | SizeTSWindowStorage or TimeTSWindowStorage                   |
   | - fixed tick: cyclic timestamp/value buffers                 |
   | - duration: timestamp/value queue buffers                    |
   | - timestamps are DateTime value elements                     |
   | - payload values are T value elements                        |
   +--------------------------------------------------------------+
   | tracking region                                              |
   | TSDataTracking { last_modified_time, parent link }            |
   +--------------------------------------------------------------+

Both models share ``TSWDataView``. The view reports elements in logical
oldest-to-newest order and exposes the same operations for size, indexed
value access, timestamps, timestamp value access, first/last element,
readiness, and mutation. ``time_at(index)`` returns the raw
``DateTime`` and ``time_value_at(index)`` returns the same stored
timestamp as a ``ValueView`` backed by the timestamp element buffer.
The layout and ops behind the view differ by schema:

.. mermaid::

   flowchart LR
      View["TSWDataView"]
      FixedOps["tick-count ops<br/>SizeTSWDataLayout<br/>fixed cyclic buffer"]
      DurationOps["duration ops<br/>TimeTSWDataLayout<br/>timestamped queue"]
      Value["value()<br/>list-shaped ValueView"]
      Delta["delta_value(t)<br/>latest element if modified at t"]

      View --> FixedOps
      View --> DurationOps
      View --> Value
      View --> Delta

For a tick-count ``TSW<T, period, min_period>``, the window is a
fixed-capacity cyclic buffer. Pushing appends while there is free
capacity and overwrites the oldest element after the period is reached.
The exposed order remains oldest-to-newest:

.. code-block:: text

   tick TSW, period = 3
   push 1 @ t1        [1]
   push 2 @ t2        [1, 2]
   push 3 @ t3        [1, 2, 3]
   push 4 @ t4        [2, 3, 4]

``all_valid()`` for this model is ``size() >= min_period``. The
``value()`` view is bound to custom list ops over the window component,
so ``value().as_list()`` has the documented list schema while reading
directly from the cyclic storage.

For a duration ``TSW<T, time_range, min_time_range>``, the window is a
queue paired with per-element timestamps. Before each push, elements
older than ``evaluation_time - time_range`` are removed. The queue may
grow to match the number of ticks observed inside the time range:

.. code-block:: text

   duration TSW, time_range = 10us
   push 1 @ 1us       [1 @ 1us]
   push 2 @ 6us       [1 @ 1us, 2 @ 6us]
   push 3 @ 16us      [2 @ 6us, 3 @ 16us]

``all_valid()`` for this model is false while empty; once non-empty, a
zero ``min_time_range`` is immediately valid and a positive
``min_time_range`` requires ``last_element_time - first_element_time``
to cover that duration.

View Handles
^^^^^^^^^^^^

View objects are handles over TSData memory; they are not embedded
inside the TSData allocation. A plain data view needs only its one-word
``TSRoleTypeRef`` and data pointer, and exposes the common time-series operations:
``type_ref()``, ``schema()``, current ``value()``, ``delta_value(t)``,
``last_modified_time()``, ``modified(t)``, ``has_current_value()``, and
``all_valid()``. Top-level input/output views should delegate this
common surface to the data view; they only add endpoint-specific
behaviour such as input binding, output mutation scopes, publication,
and subscription state.
Specialised views, for example ``TSBDataView`` and ``TSLDataView``,
wrap the plain view and provide kind-specific child access. Child
back-links are value-owned metadata stored in the child
``TSDataTracking`` record, so a projected child can outlive the
transient parent view that created it:

.. mermaid::

   flowchart LR
      View["TSDataView handle<br/>storage type<br/>data pointer"]
      Type["TSRoleTypeRef<br/>canonical TypeRecord"]
      Data["TSData storage allocation<br/>value + optional delta + tracking"]
      Tracking["TSDataTracking<br/>last_modified_time<br/>parent<br/>observers"]
      Link["TSDataParentLink<br/>tagged parent identity<br/>payload union<br/>child_id"]
      Observers["TSDataObserverSet<br/>null / single / vector"]
      ParentData["parent TSData storage"]
      Endpoint["terminal endpoint<br/>TSOutput / TSInput"]

      View -->|storage type| Type
      View -->|data_| Data
      Data -->|tracking_offset| Tracking
      Tracking -->|parent| Link
      Tracking -->|observers| Observers
      Link -->|TSData: TypeRecord + data| ParentData
      Link -.endpoint: endpoint pointer.-> Endpoint
      Link -.child_id belongs to parent.-> ParentData

The parent identity is tagged so the link carries exactly one parent
kind. For a TSData parent it stores ``TypeRecord + data``; for an endpoint
parent it stores the endpoint pointer in the same payload slot. Because
each TSData parent also stores its own ``TSDataParentLink``, a child link
can walk back to the root TSData without retaining transient views. The
walk stops at the endpoint link. The same walk produces the
root-to-child navigation path as integer field/index/slot ids.

``TSDataMutationView`` is the mutation-only handle. It carries a view
copy plus the current evaluation time and validates that the bound
``TSDataOps::allows_mutation`` property is true. Endpoint lifecycle
state, such as whether cleanup is needed after evaluation, is tracked by
the owning root ``TSOutput`` / ``TSInput`` endpoint, not by each TSData
element:

.. mermaid::

   flowchart TD
      Mutation["TSDataMutationView<br/>view_<br/>mutation_time_"]
      Tracking["view_.tracking()<br/>TSDataTracking"]
      EvaluationTime["active evaluation time"]
      Root["root endpoint state<br/>dirty cleanup coordination"]

      Mutation -->|references through view_| Tracking
      Mutation -->|carries| EvaluationTime
      Root -.coordinates mutation lifetime.-> Mutation

The active mutation time is deliberately not stored in
``TSDataTracking``. The tracking state records what happened to the
data; the mutation view records the evaluation time for the in-flight
operation.

Modification handling is deliberately split into three responsibilities.
``TSData`` tracks local modification state and per-level observers;
projecting child data records the immediate parent binding/data identity
and parent-relative child id into the child's tracking region; and child
mutations notify their parent only through ``TSDataParentLink``. The link
hides the parent-specific details by invoking the parent's
``record_child_modified(parent_data, child_id)`` hook before marking the
parent modified. Marking a TSData level modified updates
``last_modified_time`` and notifies that level's observers only on the
first modification for a given evaluation time. If the link terminates at an
endpoint parent, the endpoint records its local dirty state and the
bubble-up stops. TSData owns observer fan-out at the level being
observed, but scheduling policy and cleanup lifecycle remain the
responsibility of the surrounding endpoint / node / graph layer.

The implemented atomic plan follows the compact layout above. ``TSB``
and fixed-size ``TSL`` use the fixed structured layout above. ``TSW``
uses the window layout above. ``TSS`` and ``TSD`` use the
slot-oriented layout below: ``TSS`` tracks membership deltas with
per-slot bitsets for ``added`` and ``removed``, while ``TSD`` reuses
the same key side and adds a per-slot ``modified`` bitset for child
values that changed in the current evaluation time. The collection owns
only its collection-level ``last_modified_time``; keyed value
modification times are read from the child time-series values. The
bitset delta surface is reset at the first collection mutation for a
new evaluation time. That owner-level reset explicitly calls
``erase_pending()`` on the key store before clearing the per-slot delta
masks, so pending erases from the previous tick are released without
making the slot utility track mutation epochs. The implementation keeps
a small internal ``delta_time`` marker for that reset decision; the
public modification answer still comes only from
``last_modified_time == evaluation_time``.

Within a single evaluation cycle, structural collection changes are
netted in the slot delta masks. Adding and then removing the same key,
or removing and then re-adding the same key, clears the corresponding
``added`` / ``removed`` bit. Empty copy/apply operations also reset the
delta surface for the current evaluation time. Even when this leaves no
delta bits, the collection remains modified at that evaluation time:
touching a collection is enough to publish validity, including valid
empty ``TSS`` / ``TSD`` values.

Sequential netting is distinct from a single canonical set delta: a
delta's ``added`` and ``removed`` lists MUST be disjoint (ruling
2026-07-28) — an element listed in both is **incorrect data**, not a
state to resolve. The invariant is enforced at the construction
boundaries (the Python ``set_delta`` literal raises, the parity recipe
decoder rejects) and holds by slot-mask construction for captured
deltas; ``apply_delta`` over ``TSS`` assumes it, which makes the
application order immaterial. Released hgraph happens to tolerate the
overlapping shape by filtering both lists against prior membership in
its value setter — an accepted deviation recorded in the roadmap
(parity issues #148/#161/#162).

Slot-Oriented Collection TSData
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Collection TSData uses stable slot ids so child addresses and binding
paths do not change when the collection grows or when other keys are
removed. A slot id indexes parallel structures: key or child payload,
live/constructed state, and delta masks.

.. mermaid::

   flowchart TD
      Slot["stable slot id"]
      KeyPayload["key payload<br/>or positional child"]
      ChildPayload["child TSData<br/>where the shape has children"]
      LiveState["key constructed/live state"]
      DeltaMasks["delta masks<br/>added / removed / modified"]
      Path["binding path component"]

      Slot --> KeyPayload
      Slot --> ChildPayload
      Slot --> LiveState
      Slot --> DeltaMasks
      Slot --> Path

For a TSS, the key store owns the scalar key payload and membership
state. Added and removed deltas are bitsets indexed by the same slot
ids:

.. code-block:: text

   TSS SlotTSDataStorage
   +--------------------------------------------------------------+
   | collection tracking                                          |
   | last_modified_time                                           |
   | parent link                                                   |
   +--------------------------------------------------------------+
   | KeySlotStore                                                 |
   |                                                              |
   | slot id        0        1        2        3        ...       |
   | key bytes    [K0]     [K1]     [K2]     [K3]       ...       |
   | constructed   1        1        1        0         ...       |
   | live          1        0        1        0         ...       |
   +--------------------------------------------------------------+
   | delta bitsets for current evaluation time                    |
   | added       [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   | removed     [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   +--------------------------------------------------------------+

``constructed && !live`` represents a pending-erase slot. The payload
is still inspectable for the tick in which it was removed, and the
slot can be erased later without invalidating any other slot address.

For a TSD, the key side is TSS-shaped and the value side is parallel
payload storage indexed by the same slot ids. The key store's
``constructed`` bit is authoritative for both key and value payload
lifetime: a TSD must construct the child time-series value with the key
and destroy it when the key slot is physically erased. There is no
independent value-side constructed state in the TSData layout:

.. code-block:: text

   TSD SlotTSDataStorage
   +--------------------------------------------------------------+
   | collection tracking                                          |
   | last_modified_time                                           |
   | parent link                                                   |
   +--------------------------------------------------------------+
   | KeySlotStore                                                 |
   | slot id        0        1        2        3        ...       |
   | key bytes    [K0]     [K1]     [K2]     [K3]       ...       |
   | constructed   1        1        1        0         ...       |
   | live          1        0        1        0         ...       |
   +--------------------------------------------------------------+
   | Value payload slots                                          |
   | slot id        0        1        2        3        ...       |
   | child TS     [V0]     [V1]     [V2]     [--]       ...       |
   | lifetime      follows KeySlotStore.constructed               |
   +--------------------------------------------------------------+
   | delta bitsets                                                |
   | added       [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   | removed     [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   | modified    [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   +--------------------------------------------------------------+

The ``modified`` bitset records which child slots reported a
modification during the current evaluation time. The child's own
``last_modified_time`` still lives with the child time-series value;
the parent bitset is the parent's delta surface, not a duplicate
timestamp store.

The bubble-up path uses the slot id carried by the child's
``TSDataParentLink``:

.. mermaid::

   flowchart TD
      ChildMutation["child TSDataMutationView"]
      Mark["mark_modified() / copy_value_from()"]
      ParentLink["child tracking<br/>TSDataParentLink(parent, s)"]
      ParentHook["parent ops<br/>record_child_modified(parent_data, s)"]
      ModifiedBit["parent modified bitset[s] = 1"]
      ParentTime["parent last_modified_time<br/>= current evaluation time"]
      Bubble["repeat with parent's parent"]
      Endpoint["terminal endpoint<br/>mark output dirty"]

      ChildMutation --> Mark
      Mark --> ParentLink
      ParentLink --> ParentHook
      ParentHook --> ModifiedBit
      ModifiedBit --> ParentTime
      ParentTime --> Bubble
      Bubble --> Endpoint

Builder Lifetime
----------------

Time-series endpoint builders are reusable builders. A
``TSOutputBuilder`` resolves a ``TSValueTypeMetaData`` schema to a role record
for every normal time-series root, plus the endpoint state needed to construct
a top-level output. Legacy bindings remain only in internal compatibility and
alternative-representation paths. A ``TSInputBuilder`` resolves a
``TSInputConstructionPlan`` compiled from ``TSValueTypeMetaData`` plus
``TSEndpointSchema`` annotations. That plan describes the non-peered
input tree and peered terminals needed to construct a top-level input.
Once resolved, endpoint builders should be cached and reused to
construct multiple endpoints with the same endpoint plan. This is the
opposite of the value-layer ``ListBuilder`` / ``MapBuilder`` family,
which is local scratch storage for one immutable ``Value``.

This distinction matters most for nested structures. A ``TSB`` builder owns
the reusable builders for its fields; a fixed ``TSL`` builder owns the
reusable builder for each element position; a ``TSD`` builder owns the
reusable value-side time-series builder used whenever a new key appears.
For inputs, the endpoint annotation graph owns non-peered TSB /
fixed-TSL prefixes and peered terminals instead of value payload
storage. The builder graph is shared construction metadata, while each
endpoint instance owns its planned input storage independently. Output
endpoints own current/delta payload storage; input endpoints borrow
output payloads through target links.

Memory Stability Invariant
--------------------------

Every time-series value in the runtime must be memory-stable. Once an
output's ``TSData`` and storage identity are published, their addresses must not
move for the lifetime of the owning node. Output-to-input binding is
implemented by recording borrowed handles to output TSData positions,
and those handles must remain valid across ticks, rebinds, and
structural mutation of containers.

This is a **TSData invariant**, not a general rule for unrelated
scalar ``Value`` objects. The scalar value layer may use compacting or
move-based storage when the value schema permits it. Time-series
storage is different because inputs, child views, proxies, observers,
and parent links borrow concrete TSData addresses across evaluation
cycles.

For fixed-shape time-series (``TS``, ``TSB``, and fixed-size ``TSL``),
stability is trivial: the value lives in node-owned storage and
survives until the owning node is destroyed. Tick-count ``TSW`` also
has a fixed-capacity window. Duration ``TSW`` keeps the owning TSData
object stable, but its internal queue may grow; callers should treat
element ``ValueView`` handles as short-lived projections rather than
stable child time-series addresses.

For TSD, stability is harder. Elements are added and removed during
evaluation, but a consumer that bound to one of them on the previous
tick must still be able to dereference it on the current tick.
Compacting storage cannot be used. The runtime instead uses chained,
non-relocating slot blocks: new capacity is appended without moving
previously published slot addresses.

Dynamic ``TSL`` has indexed growth but no removals today. It keeps child
TSData storage stable by allocating each child behind its own TSData
heap storage handle; growing the parent list can relocate handles, but
not the child allocations they reference. The dynamic ``TSL`` storage
plan also withholds erased copy/move lifecycle hooks.

.. _ts-path-construction:

Path Construction and the Slot Concept
--------------------------------------

Every binding carries a path that locates the target value within its
owning graph. For indexable kinds — TSB, TSL — the path is a sequence
of ``size_t`` indices (field index, element index) and addressing is
direct.

TSD breaks this because keys can be of any scalar type. Carrying
arbitrary keys in paths would be expensive (variable-width encoding,
type-aware comparison at every step) and would couple every path
traversal to key semantics.

The runtime resolves this by introducing the **slot**: a stable
non-negative integer that names an element of a keyed container
without referencing the key itself. A slot is allocated when a key is
first inserted, remains allocated while the key is live, and persists
through delayed-erase windows so consumers can still inspect a removed
element on the tick of its removal. With slots, paths into TSD become
``(slot_id)`` — exactly the shape of paths into TSL — and the runtime
can treat all keyed containers uniformly.

Slots originate in TSS rather than TSD. The reason is the TSS/TSD
relationship described below: a TSD exposes its keys as a TSS, and
that TSS must use the same slot ids as the parent TSD. Putting slots
at the TSS layer first lets TSD's value side reuse them directly.

.. _ts-slot-store-family:

The Slot Store Family
---------------------

Layered utilities in ``hgraph/types/utils/`` express the slot machinery.
The ``SlotTSDataStorage`` implementations for collection-shaped
time-series data build on these primitives so they can support
per-element insert / remove / replace with stable addresses and the
per-slot bitsets needed to surface deltas. The value-layer
(scalar) container shapes are different — they are compact and atomic
by design (see *Scalar Plans and Ops > Container Storage Shapes*) and
do not use the slot stores.

``StableSlotStore<StateModel>``
    The reusable payload-type-erased contract for non-relocating slots.
    ``slot_id`` maps through a replaceable pointer table into chained payload
    blocks, so growth never moves previously published payloads. The public
    contract is declared separately from its concrete strategies under
    ``hgraph/types/utils/impl``.

    The facade is one tagged implementation pointer. Its private tag selects
    canonical nop, tagged-pointer, or bitmap behaviour through an inline
    switch. Semantic owners therefore do not contain a ``std::variant`` or
    name either representation; concrete classes remain under the ``impl``
    boundary. The implementation allocation still uses the supplied
    ``MemoryUtils::AllocatorOps``.

    Strategy selection happens once when the immutable ``StorageLayout`` is
    bound. Payloads aligned to at least ``uintptr_t`` use two low pointer bits
    for ``live`` (``00``), pending erase, staged construction, and free. A
    known-live access can therefore use the encoded word directly. Weaker
    alignment preserves the payload's compact stride and uses the existing
    raw pointer table with one constructed bitmap, or constructed and live
    bitmaps when delayed erase is required. The implementation deliberately
    does not over-align weak payloads merely to enable tagging.

``StableSlotStorage``
    Lower-level allocation-only compatibility utility. It exposes a raw slot
    pointer table and chained blocks but does not model lifecycle state. New
    lifecycle-owning structures should use ``StableSlotStore`` instead.

``KeySlotStore``
    Stable slot-backed key storage with delayed-erase semantics. Owns
    homogeneous keys keyed off a ``StoragePlan`` and a small ops vtable
    (``hash``, ``equal``). Its full lifecycle state model exposes:

    - ``constructed[slot]`` — payload object exists in slot memory
    - ``live[slot]`` — payload is currently a member of the set

    A slot in ``constructed && !live`` is *pending erase*: still
    addressable and inspectable until the owner explicitly calls
    ``erase_pending()``. TSData calls this when a new evaluation-time delta
    epoch begins, before resetting its per-slot delta masks. This is
    what lets a consumer that bound on the previous tick inspect the
    slot's last value during the tick of its removal without making the
    utility store track mutation epochs.

``ValueSlotStore``
    Standalone parallel value memory keyed off externally supplied slot
    ids. As a reusable utility it owns per-slot constructed state
    so it can be used independently and still destroy its payloads
    correctly. A TSD-specific value side should not treat that bit as a
    second source of truth: TSD key construction and value construction
    happen together, so ``KeySlotStore.constructed`` is authoritative
    and any reused value-store constructed state is only a derived mirror.

``KeyMirroredValueSlotStore``
    Wrapper for keyed value storage that enforces the TSD-style
    lifetime rule in code. It registers as a ``KeySlotStore`` observer,
    constructs value payloads when key slots are constructed, keeps
    pending-erase value payloads alive while the key slot remains
    constructed, and destroys value payloads only when the key slot is
    physically erased or cleared. Its public ``has_slot`` answer is
    derived from ``KeySlotStore.constructed``.

Both stores expose a ``SlotObserver`` notification protocol —
``on_capacity``, ``on_insert``, ``on_remove``, ``on_erase``,
``on_clear`` — so parallel structures over the same slot ids stay
synchronised without any of them needing to know about the others.
The TS layer uses this for two purposes: a ``MapValueObserver``
mirrors a key store's slot lifecycle onto a paired value store
(``TSD`` keys → values); and delta-recording observers capture
``TSS`` ``added`` / ``removed`` slot ids plus ``TSD`` ``modified``
slot ids for the current evaluation time so the layer can publish
``delta_value``.

These structural slot observers are internal synchronisation hooks, not
the public change-notification surface. Per-level change propagation
uses the ``TSDataParentLink`` stored in child tracking plus the
``record_child_modified`` hook on the parent ops table. The parent link
owns the child id because that id is a parent-local slot/path
identifier. It can also resolve the root ``TSDataView`` and the
root-to-child navigation path by walking the chain of TSData parent
links and stopping at the endpoint parent.
The slot hooks may update bitsets immediately during
mutation; processing of modified elements and external notification
fan-out belongs to the surrounding state/value layer after the
value-level mutation count returns to zero.

The set and map ``SlotTSDataStorage`` shapes are layered on these
primitives. A TS set data store owns one ``KeySlotStore``. A TS map
data store owns one ``KeySlotStore`` for keys plus value payload
storage indexed by the key slots. If the generic ``ValueSlotStore`` is
reused for that value side, use ``KeyMirroredValueSlotStore`` or the
same rule internally: its constructed state must be kept as a strict mirror
of the key store's constructed state rather than a separate state surface.

Slot stores are deliberately **not** used for scalar values. The
delayed-erase, per-slot-bit, and observer machinery exists to support
delta tracking across ticks; for non-time-series payloads that
machinery is overkill and a plain ``ErasedOwner`` suffices (see
*Scalar Plans and Ops*).

TSS Storage
-----------

TSS is the time-series wrapper around a delta-tracking Set. It owns:

- a ``KeySlotStore`` for the keys, providing stable per-slot addresses
  and delayed-erase semantics;
- collection-level tracking, including ``last_modified_time``;
- per-slot ``added`` and ``removed`` bitsets that drive
  ``delta_value``.

The slot ids assigned by the ``KeySlotStore`` are the path identifiers
used throughout the rest of the time-series layer. A TSS has no child
time-series values, so key-level modification time is not a concept on
this storage shape.

A TSS instance can be **owning** — a TSS output written by a node — or
**read-only** — a TSS view exposed by another container (see TSD).
Both modes share the same TSS surface: ``value``, ``delta_value``,
subscription, and slot-based path access. The read-only mode rejects
write operations because the underlying storage belongs to the parent
container.

The C++ TSData API uses the standard set-view names:
``TSDataView::as_set()``
returns ``TSSDataView`` with ``size()``, ``empty()``, ``contains()``,
``find_slot()``, ``values()``, ``added_values()``,
``removed_values()``, ``added()``, ``removed()``,
``slot_added()``, and ``slot_removed()``. ``TSSDataMutationView`` adds
``add()``, ``remove()``, ``clear()``, and ``reserve()``.

TSD Storage
-----------

TSD is the time-series wrapper around a delta-tracking Map. It owns:

- a ``KeySlotStore`` for the keys, identical in shape to a TSS's;
- value payload storage whose slot ids match the key store's, holding
  the per-key time-series values and deriving payload lifetime from
  ``KeySlotStore.constructed``;
- collection-level tracking, including ``last_modified_time``;
- the TSS-shaped per-slot ``added`` and ``removed`` key bitsets;
- a per-slot ``modified`` bitset for keys whose child time-series
  value modified in the current evaluation time.

The value side is itself a recursive time-series layer: each value-
slot holds a complete time-series value (most often a ``TS``, but
``SIGNAL``, ``TSS``, ``TSB``, fixed ``TSL``, ``TSW``, or further nested
``TSD`` are all permitted by the schema). Memory stability is preserved by the underlying
``StableSlotStore`` so consumers can bind to a specific slot's value
without worrying about future structural changes.

Slot-backed TSData must be updated in place. Its erased storage plans
must not expose copy or move lifecycle hooks: those hooks would let
generic code relocate a published slot store or replace a child
time-series allocation behind an existing binding. Whole-collection
assignment is therefore implemented by TSData mutation views
(``add``/``remove``/``set``/``erase`` and delta application), never by
copying or moving the storage object. If a helper cannot preserve this
rule, it must reject the operation rather than synthesize a replacement
collection.

Per-key modification time is read from the child value stored in the
matching value slot. The TSD-level ``modified`` bitset is the current
delta membership surface; it is not the source of the child's
``last_modified_time``.

Key-Set Exposure
~~~~~~~~~~~~~~~~

A TSD exposes its keys as a TSS through ``key_set()``. The returned
TSS is **read-only**:

- it shares the parent TSD's ``KeySlotStore`` directly, so slot ids
  match one-to-one with the parent's value side;
- it shares the parent TSD's key ``added`` / ``removed`` tracking;
- it can be subscribed to and exposes ``value`` and ``delta_value``
  like any other TSS;
- it rejects write operations — keys are owned by the TSD and only
  change through the TSD's mutable view.

This is the value-layer Map → read-only Set view (described in
*Erased Types*) lifted into the time-series layer. The difference is
that the time-series version carries modification time and a delta
stream, not just structural read access.

The C++ TSData API uses the standard dictionary-view names:
``TSDataView::as_dict()`` returns ``TSDDataView`` with keyed
``contains()``, ``find_slot()``, ``at(key)``, ``keys()``, ``values()``,
``items()``, ``valid_keys()``, ``valid_values()``, ``valid_items()``,
``modified_keys(evaluation_time)``, ``modified_values(evaluation_time)``,
``modified_items(evaluation_time)``, ``added_keys()``,
``added_values()``, ``added_items()``, ``removed_keys()``,
``removed_values()``, ``removed_items()``, and ``key_set()``.
``TSDDataMutationView`` adds ``set(key, value)``, ``erase(key)``,
``clear()``, and ``reserve()``. Child mutation bubbles through the
child view's parent link and records the parent's ``modified`` bit for
the child slot. The explicit ``evaluation_time`` on modified ranges is
the TSData-layer form of querying the current evaluation time.

Buffer Exposure
~~~~~~~~~~~~~~~

Because keys and values live in slot stores backed by stable
contiguous blocks, both TSS and the value side of TSD can expose
buffer views the same way the value layer does — over live keys, over
live values, and over the per-slot delta masks for the current tick:
``added`` / ``removed`` for TSS-shaped key storage and ``modified`` for
TSD value changes. This matters for adaptors and analytics paths that
consume large keyed time-series in bulk.

Delta Visibility and Lazy Cleanup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A delta is **only visible in the cycle the structure was modified**. This is
the same rule as the atomic ``delta_value(t)`` gate (it returns a typed-null
view unless ``last_modified_time == evaluation_time``), applied uniformly to
*every* delta surface: ``delta_value``, and the keyed ``added`` / ``removed`` /
``modified`` keys / values / items.

The split of responsibility is deliberate:

- The data-layer ``TSDDataView`` / ``TSSDataView`` ``added()`` / ``removed()``
  (which take no time argument) expose the **raw** physical key/slot bitsets.
  The data-layer ``modified_*`` accessors take an explicit ``evaluation_time``
  and already gate on it.
- The endpoint views (``TSDInputView`` / ``TSDOutputView`` /
  ``TSSInputView`` / ``TSSOutputView``) gate **all** of their delta accessors on
  ``modified()`` (the structure's ``last_modified_time == evaluation_time``).
  A structure that was not modified this cycle therefore reports an empty
  ``added`` / ``removed`` / ``modified`` set even though the underlying bitsets
  still physically hold the previous cycle's values. This is what consumers
  (operators, nested-graph forwarding, mesh sibling reads) must see, and it
  removes any need for an eager reset of the delta before the next mutation.

There is **no end-of-cycle cleanup sweep**. Physical reclamation is lazy and
happens on the structure's **next mutation**: the slot-store ``prepare_delta``
resets a collection's ``added`` / ``removed`` / ``modified`` bitsets and erases
pending-removed keys the first time it is touched at a **newer**
``delta_time_`` (the ``modified_time <= delta_time_`` no-op guard, which is
also the per-collection optimisation that skips re-clearing a delta already
reset at ``t``). Because reads are cycle-gated, a stale delta is never
observable in the window between the cycle it was produced and the producer's
next mutation, so no eager reclamation pass is required.

Delta clocks and delta windows are **monotonic**. A record that carries an
*older* time than the current window — the canonical producer is a freshly
bound link replaying its source's historical timestamp (plain ``bind``
semantics), e.g. a re-homed child terminal aliasing an already-ticked source —
**joins the current window instead of rebasing it**: ``prepare_delta`` treats
``modified_time <= delta_time_`` as the current window, and
``TSDataTracking::record_modified`` ignores older-than-recorded times (no
rewind, no stale re-notification of observers). Rebasing on a stale record
would erase sibling marks already recorded this cycle — the failure mode
behind issue #38, where a switch branch-flip's sampled structural transition
and a sibling key's added/modified marks were wiped by exactly such a replay,
losing the delta downstream (dedup saw "no change") while the value read
correctly. The dynamic-TSL modified ring drops stale records instead of
joining (re-appending an already-linked entry would corrupt the ring); the
``TSDProxy`` updated-window roll follows the same newer-only rule. There is deliberately **no** ``cleanup_delta`` ops entry on
``TSDataView`` / ``TSOutput`` / ``NodeView`` and **no** ``TSOutput::dirty_``
flag: the earlier eager-sweep apparatus has been removed in favour of this
read-gated, mutation-driven model.
