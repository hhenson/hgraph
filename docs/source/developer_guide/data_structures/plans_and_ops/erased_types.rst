Type-Erased Data Structures
===========================

Both scalar and time-series implementations expose their values through
type-erased handles rather than concrete C++ templates. This chapter
describes the erased surface — the value-side handle, the view, and the
ops vtable — that every kind reuses. The concrete layouts for scalar
and time-series kinds are described in the next two pages.

Why type erasure
----------------

A single kind can have several implementations, chosen by the caller's
needs and bound at the value layer's resolution step.

Pure-data callers — those that only need to store, hash, compare, and
serialise values atomically (read whole, replace whole) — use
**compact** implementations. A pure-data Set is a hash-backed key
store; a pure-data Map is a contiguous keyed map; pure-data
List / CyclicBuffer / Queue use contiguous element arrays. These
forms are described in *Scalar Plans and Ops > Container Storage
Shapes*. They minimise memory and skip the per-element bookkeeping
the value layer never needs.

Time-series callers — those that need to observe insertions,
removals, and modifications across ticks — use **slot-store-based**
implementations. The slot-store-based Set is the storage substrate
for ``TSS``; the slot-store-based Map is the substrate for ``TSD``.
They carry per-slot observers, stable-address slots, and pending-
erase state so the time-series layer can produce coherent deltas.
These forms are described in *Time-Series Plans and Ops > The Slot
Store Family*.

Both modes expose the same ``SetView`` / ``MapView`` shape, but with
different mutation surfaces: the pure-data view does whole-container
replace; the slot-store view does per-element insert / remove. Code
that *reads* a Set or a Map through its view does not need to know
which implementation it is looking at. The choice is made by the
schema's bound ``StoragePlan`` and ``ValueOps``; the read contract
is unchanged.

This is the load-bearing reason values are type-erased rather than
generic-templated: it lets the time-series layer reuse the value-layer
container vocabulary without forking the surface.

Type record: the shared anchor
------------------------------

Value type erasure is held together by the common ``TypeRecord``. It records
the schema, family role, storage plan, ops table, capabilities, and optional
debug descriptor for one implementation:

.. code-block:: cpp

   struct TypeRecord {
       TypeRole                        role;
       TypeCapabilities                capabilities;
       const SchemaHeader*             schema;
       const MemoryUtils::StoragePlan* plan;
       const void*                     ops;
       const DebugDescriptor*          debug;
   };

   class ValueTypeRef {
       const TypeRecord* record;
   };

A ``ValueTypeRef`` is a one-word, trivially-copyable typed reference to a
value-family ``Instance`` record. ``Value``, ``ValueView``, and builders use
it to reach schema, plan, lifecycle, capabilities, and ``ValueOps``.

``intern_value_type`` interns one record per ``(schema header, Instance role,
plan, ops)`` key in ``TypeRecordRegistry``. ``ValuePlanFactory`` caches
``schema -> ValueTypeRef`` by value; there is no value-family binding side
registry.

Composite types populate lazily. The first ``type_for`` request for a tuple,
list, map, or other container schema synthesises its plan from child type
refs and interns the resulting common record.

The View
--------

A view is the type-erased non-owning handle:

.. code-block:: cpp

   class ValueView {
       ValuePtr pointer; // tagged TypeRecord pointer + borrowed data pointer
   };

The record gives the view its schema, plan, lifecycle, capabilities, and ops;
the data pointer addresses the borrowed payload. ``ValueView`` is exactly two
words and is move-only. Specialised view adapters carry at least this context
and may cache resolved ops or layout facts established at construction time.

Record structure and value-family identity are validated when records are
interned and when a generic ``AnyPtr`` is narrowed with
``ValueTypeRef::checked``. A ``ValueView`` constructed from that trusted
``ValueTypeRef`` reads its record, payload, and access tag directly; ordinary
``valid()``, ``has_value()``, and typed value operations do not repeat
``TypeRecord::valid`` on every access. Explicit access-mode transitions, such
as opening a mutation view, retain their transition checks.

A view exposes:

- type interrogation: ``is_atomic()``, ``is_tuple()``, ``is_list()``,
  …;
- typed access: ``as<T>()``, ``try_as<T>()``, ``checked_as<T>()`` for
  atomic kinds;
- generic ops: ``hash()``, ``equals()``, ``compare()``, ``to_string()``,
  ``clone()``, ``copy_from()``, ``try_copy_from()``
  — routed through the type record; ``compare()`` returns
  ``std::partial_ordering`` as the common erased representation of
  ``operator<=>`` results. Compact containers use their bound ops table,
  while structured tuple/bundle/fixed-list views recurse through child
  views;
- Python bridge conversion, when enabled: ``to_python()`` and
  ``from_python()``;
- read access for composite kinds via specialised adapters described
  below.

Atomic ``set<T>`` is available only on a mutable ``ValueView`` opened
with ``begin_mutation()``. Structural mutation and delta views are not
part of the scalar value view. Delta views are reserved for the
``TSOutput`` / ``TSInput`` view infrastructure where per-tick
modification state is meaningful.

View Casting
------------

The erased layer supports direct casting from one view shape to
another so callers do not need to chain calls to reach a typed
handle. Two cast families exist:

- **Kind-specialised view casts**: ``as_atomic()``, ``as_tuple()``, ``as_bundle()``,
  ``as_list()``, ``as_set()``, ``as_map()``, ``as_cyclic_buffer()``,
  ``as_queue()``, ``as_any()``, with ``try_as_*`` counterparts that return
  ``std::optional`` and throw nothing.
- **Atomic typed casts**: ``as<T>()``, ``try_as<T>()``,
  ``checked_as<T>()`` reach the underlying scalar in one call.

Both families are mirrored on the owning ``Value`` itself
(``Value::as_list()``, ``Value::as<T>()``, …) so callers holding a
``Value`` do not need to dereference into a ``ValueView`` and then
cast again.

These casts only re-interpret the existing type record's view shape. They
do not change the underlying schema or copy the payload. Cross-schema
adaptation — exposing one schema's value through a different schema —
is a time-series concern, not a value-layer concern.

Status: the read-only cast family is implemented for atomic, tuple, bundle,
list, set, map, cyclic buffer, queue, and Any views. Mutable-view casts are
implemented as casts from an already-open mutable erased view; opening
that mutable view is always done by ``begin_mutation()`` and is gated
by the bound ops table.

Read-Only and Mutable Views
---------------------------

Views conceptually come in read-only and mutable variants. The
distinction is part of the public contract, not just C++ ``const``
discipline:

- A **read-only view** exposes inspection and iteration: typed
  access, ``hash``, ``equals``, ``compare``, ``to_string``, buffer
  exposure, and structural reads.
- A **writable view** is a read-only view that was created from
  writable storage, but mutation has not been opened yet. Mutating
  methods still fail in this state.
- A **mutable view** adds the kind-specific mutation operations:
  scalar ``set<T>``, field mutation on bundles and tuples,
  ``push_back`` / ``resize`` on lists, ``add`` / ``remove`` on sets,
  key insertion and value updates on maps, and so on.

The generic erased handle can represent writable and mutable states
without adding a third pointer word: the type-record pointer carries a
small tag. A mutable view is obtained from a writable view by calling
``begin_mutation()``. The transition is explicit so that consumers can
reason about when mutation is in scope — the time-series layer in
particular needs to know precisely when changes start and end so its
delta accounting stays coherent.

Whether ``begin_mutation()`` is legal is a property of the bound ops
table. Atomic, tuple/bundle, and fixed-array ops may allow direct
in-place mutation. Compact container storage ops deliberately set this
flag to false, so ``ListStorage``, ``SetStorage``, ``MapStorage``,
``CyclicBufferStorage``, ``QueueStorage``, and map-key-set adapters
remain immutable from the public API.

At the time-series layer the same idea gates a **typed fast write**:
``TSDataOps.direct_native_value`` marks storages whose current value is
a pure native slot — assigning it in place, inside an active mutation,
preserves every representation invariant. For those,
``TSDataMutationView::mutable_value()`` hands out a writable value view
and the caller commits with ``mark_modified()`` (which performs the
modification recording, observer notification, and parent bubbling the
erased ``copy_value_from`` path would have done). ``Out<TS<T>>::set``
uses this pair when the realized value ops match ``T`` exactly and
falls back to the erased copy otherwise — python-cached and
python-owned storages keep ``direct_native_value`` false because their
writes carry cache-invalidation side rules (2026-08-15; motivated by
the recordable-counter write cost surfaced in the std-operator audit).

The mutation is closed by calling ``end_mutation()`` on the mutable
view. For the current scalar value-layer ops this is a no-op; the
method exists so the same view contract can be used by slot-store-
backed time-series ops, where close-time hooks update delta state.
Those future mutable views should also provide RAII closure so a
mutation is not left dangling if a caller forgets to close it
explicitly or an exception unwinds the stack.

.. note::

   **Expected use: one outer mutation per evaluation cycle.** The runtime
   contract assumes that a given collection enters at most one
   *outermost* ``begin_mutation()`` per evaluation cycle. Nested
   ``begin_mutation()`` calls within that outer scope are fine — they
   are tracked as a depth counter and only the outermost call performs
   start-of-mutation work — but the design does not anticipate a
   collection completing one full mutation and then opening a second
   independent mutation in the same cycle. Pending-erase cleanup runs
   at the start of an outermost mutation, so a second outermost
   mutation in the same cycle would re-run the cleanup against the
   delta state still in scope for the current tick and discard
   information consumers may still want to read.

   If a use case ever needs multiple disjoint outer mutations per
   cycle, the fix is not to disable cleanup but to change mutation
   tracking from a depth counter to an *evaluation-time* stamp: record
   the cycle in which cleanup last ran, and let the start-of-mutation
   logic skip work whenever that stamp matches the current
   ``evaluation_time``. Subsequent outer mutations within the same
   cycle would then be no-ops for cleanup while still flushing user-
   visible changes on close. This is a deliberate fallback path; the
   current depth-counter implementation is correct as long as the
   one-outer-mutation-per-cycle assumption holds.

One specific cross-kind read-only view is worth calling out:
``MapView`` exposes ``key_set()``, which returns a read-only
``SetView`` over the map's keys. Callers can iterate or query keys
with the same surface they would use for a standalone set, without
copying or materialising a second container. The view is read-only
because the keys belong to the map; structural changes go through the
map's mutable view.

Specialised Views
-----------------

Each kind has a specialised **read-only** view that adds kind-specific
access on top of ``ValueView``. Specialised views hold at least the
same ``ValuePtr`` context as ``ValueView`` and may cache
resolved ops pointers or other construction-time facts to keep later
calls free of repeated validation. Most share an ``IndexedValueView``
base for the kinds that are addressed positionally.

The base specialised views never expose mutation methods other than
the explicit transition call. Mutation goes through a separate
**mutable** view obtained from the read-only/writable view by calling
``begin_mutation()`` (see *Read-Only and Mutable Views* above). The
mutable view is closed with ``end_mutation()`` and is the only place
per-element ``set`` / ``insert`` / ``remove`` / ``push_back`` style
operations exist. Compact value-layer container storage does not allow
that transition; replacement happens at the ``Value`` level
(whole-container copy/move or ``from_python()``).

Read-only views
~~~~~~~~~~~~~~~

``AtomicView``
    Shape tag for the open-ended atomic kind. It retains the
    ``ValueView`` scalar access operations rather than enumerating known C++
    alternatives, so independently registered extension scalars remain
    first-class.

``IndexedValueView``
    Base for tuple, bundle, list, cyclic buffer, and queue views.
    Adds ``size()``, ``at(index)``, ``operator[](index)``, and a
    forward iterator over child ``ValueView`` handles. Resolves the
    per-element ``ValueTypeMetaData`` either from the field array
    (tuple, bundle) or from the homogeneous element type (list,
    cyclic buffer, queue). Read-only — per-index ``set`` is on the
    mutable variant.

``TupleView``
    Index-addressed positional fields. Field types may differ.

``BundleView``
    Named tuple. Adds ``has_field(name)``, ``field(name)``,
    ``at(name)`` / ``operator[](name)`` for name-addressed access on
    top of the indexed surface.

``ListView``
    ``size()``, ``at(index)``, ``front()``, ``back()``,
    ``is_fixed()``, ``element_schema()``, iteration. Read-only.

``CyclicBufferView``
    Read surface plus ``capacity()``, ``empty()`` / ``full()`` and
    ``head`` (the ring's logical start). Iteration is in ring order.

``QueueView``
    Read surface plus ``size()``, ``empty()`` / ``full()``,
    ``front()`` (returns a child view of the front element).

``SetView``
    ``contains(key)``, ``element_schema()``, ``values()``, iteration
    over members. ``contains`` is part of the erased ops contract and
    must be average O(1) for set
    implementations. Set comparison orders by size when sizes differ;
    same-sized sets compare equivalent when their members match and
    unordered otherwise.

``MapView``
    ``contains(key)``, ``at(key)`` / ``operator[](key)``, iteration
    over ``(key, value)`` entries via ``entries()`` / ``items()``,
    ``keys()``, ``values()``, ``key_schema()``, ``value_schema()``,
    and ``key_set()`` returning a read-only ``SetView`` over the live
    keys. ``contains`` and ``at`` are part of the erased ops contract
    and must be average O(1) for map implementations.

``AnyView``
    View over an ``Any`` box (see *Schemas > Scalar Schemas > Value
    Kinds*). ``has_value()`` reports whether content has been assigned;
    ``get()`` returns a read view of the contained value (an invalid view
    when empty); ``value_schema()`` returns the contained value's schema or
    ``nullptr``. The mutable counterpart ``MutableAnyView`` (via
    ``begin_mutation()``) adds ``set(value)`` (replace, deep copy) and
    ``clear()`` (return to empty). Unlike the compact containers, the
    ``Any`` ops allow ``begin_mutation()``, so the box is reassignable.

Value visitation and operation ownership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``hgraph::visit`` from ``<hgraph/types/value/visitor.h>`` dispatches a live
``ValueView`` to ``AtomicView``, ``TupleView``, ``BundleView``, ``ListView``,
``SetView``, ``MapView``, ``CyclicBufferView``, or ``QueueView``. A
shape-specific callable takes precedence over a ``ValueView`` catch-all.
Every reachable branch returns ``void`` or the same safe owned type.
Reference and lazy hgraph range results are rejected because the selected
specialised wrapper is a temporary borrowed cursor.

``Any`` is deliberately transparent in this dispatch. The visitor repeatedly
obtains the contained view until it reaches a concrete semantic shape.
``AnyView`` remains necessary for box management—empty-state inspection,
replacement, and clearing—but is not a visitor alternative. An empty box and
any other absent payload cannot be visited.

Dispatch uses the declared value kind and does not call ``concrete()``.
Consequently enums remain atomic, shaped arrays remain lists, and owned
recursive records retain their bundle surface. Concrete projection and
recursive child walking are caller policies.

The visitor complements rather than replaces value type erasure. Hashing,
equality, comparison, formatting, conversion, copying, and assignment remain
in ``ValueOps``. Schema factories, codec planning, and wiring-time overload
selection continue to inspect metadata directly. Use visitation only for
caller-owned algorithms whose behaviour genuinely varies by a live value
shape.

After the kind switch, the visitor constructs specialised views through a
trusted internal projection that preserves the original access tag and avoids
repeating semantic-kind validation. The specialised view still resolves and
checks the operation-table subclass required by its methods. Visiting writable
storage does not implicitly enter mutation; ``begin_mutation()`` remains an
explicit transition.

Mutable views (ops-gated)
~~~~~~~~~~~~~~~~~~~~~~~~~

Each mutable counterpart is obtained from its read-only view via
``begin_mutation()`` and adds the mutation methods listed below; the
read surface stays available throughout the mutation scope. The
methods listed are *additions* — read-only methods on the base view
remain accessible through the mutable view.

Status: atomic views, tuple/bundle views, and fixed-array list views
currently support explicit mutation when their ops table allows
``begin_mutation()``. The compact (build-once) container storage ops do
not allow it. Structural mutation is supported for **mutable** container
schemas (``ValueTypeFlags::Mutable``), which bind to slot-store-backed
storage and install a structural-mutation ops surface — the mutable list
is implemented (below); the mutable map follows; the remaining structural
methods are still design/API targets for the slot-store-backed
time-series layer.

``MutableTupleView``
    Adds mutable child access by index. ``set(index, value)`` is a
    time-series-layer target.

``MutableBundleView``
    Adds mutable child access by index/name. ``set(index, value)`` and
    ``set(name, value)`` are time-series-layer targets.

``MutableListView``
    Adds mutable child access by index. For a **mutable** list
    (``ValueTypeFlags::Mutable``, from ``registry.mutable_list``) the
    structural methods are **implemented** — ``push_back(value)``,
    ``set(index, value)``, ``erase(index)`` (shifts later elements down),
    ``pop_back()`` and ``clear()`` go through the ``MutableListValueOps``
    surface over the growable slot-store storage. On a compact (immutable)
    list these throw.

``MutableCyclicBufferView``
    Structural ``push_back(value)`` (replaces oldest when full) and
    ``set(index, value)`` are time-series-layer targets.

``MutableQueueView``
    Structural ``push(value)`` and ``pop()`` are time-series-layer
    targets.

``MutableSetView``
    Adds ``insert(key)``, ``remove(key)``.

``MutableMapView``
    For a **mutable** map (``ValueTypeFlags::Mutable``, from
    ``registry.mutable_map``) this is **implemented** — ``set_item(key,
    value)`` (insert-or-replace), ``remove(key)`` and ``clear()`` go through
    the ``MutableMapValueOps`` surface over a ``KeySlotStore`` +
    ``ValueSlotStore`` (the same slot substrate the time-series layer uses,
    without delta). The keys remain owned by the map; structural changes flow
    through the mutable view rather than through the ``key_set()`` accessor
    (which always returns a read-only ``SetView``). On a compact (immutable)
    map these throw.
