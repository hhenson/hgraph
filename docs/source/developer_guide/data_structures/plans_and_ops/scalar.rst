Scalar Plans and Ops
====================

The scalar (value) layer is the simplest concrete instantiation of the
Plan/Schema/Ops/Builder/Value/View vocabulary: each role maps onto one
named runtime element, with no time-series state or per-tick
bookkeeping.

Mapping to Core Concepts
------------------------

================  ============================================================
Concept role      Value-layer name
================  ============================================================
Schema            ``ValueTypeMetaData`` — kind, capability flags, fields,
                  element/key types. *No layout information; that lives on
                  the matching* ``StoragePlan``.
Plan              ``MemoryUtils::StoragePlan`` — memory layout (size,
                  alignment, field offsets) plus a ``LifecycleOps`` table
                  (construct, copy/move construct and assign, destroy).
                  Carries no allocator reference.
Plan factory      ``ValuePlanFactory`` — schema → plan mapping and
                  canonical ``ValueTypeRef`` resolution, with results
                  cached against the schema. Atomic plans/types are
                  pre-registered by ``TypeRegistry`` from
                  ``MemoryUtils::plan_for<T>()`` and ``ops_for<T>()``;
                  composite and container plans/types are
                  synthesised on first use.
Ops               ``ValueOps`` — ``hash``, ``equals``, ``compare``
                  returning ``std::partial_ordering``, ``to_string``
                  function pointers
Type              ``TypeRecord`` — canonical common record for the
                  ``(schema, Instance role, StoragePlan, ValueOps)``
                  implementation. ``ValueTypeRef`` is its one-word typed
                  handle.
Builder           Per-kind value builders — wrap type refs, accumulate
                  construction-time scratch storage, and construct
                  immutable ``Value`` instances. These are local,
                  single-use instance assemblers, not cached reusable
                  builders.
Value             ``Value`` — owning handle over storage + type record + allocator
View              ``ValueView`` — base two-word reference:
                  a ``ValuePtr`` containing ``(tagged TypeRecord, data)``.
                  Specialized adapters extend it
                  for composite kinds and may cache resolved ops/layout
                  facts.
================  ============================================================

The schema-side details (``ValueTypeMetaData`` fields, kinds, composite
synthesis) are described in *Schemas > Scalar Schemas*. The erased view
surface, multi-implementation rationale, view casting, and mutable-view
contract are described in *Erased Types*. The remaining pieces — plan
factory, owning value, storage, buffer/Arrow interop, nullability — are
covered below.

Plan Factory
------------

``ValuePlanFactory`` is the schema → plan mapping and the default
schema → ``ValueTypeRef`` resolver. It is the value layer's answer to "the
schema is independent of plan, but a builder needs to pick a plan and
ops surface": the factory hands back the canonical plan or canonical
common ``TypeRecord`` through a one-word ``ValueTypeRef``, with results
cached by value against the schema.

- **Atomic schemas** are paired with their canonical plan at
  registration time. ``TypeRegistry::register_scalar<T>()`` calls
  ``ValuePlanFactory::register_atomic(schema, &MemoryUtils::plan_for<T>(),
  &ops_for<T>())``. This interns the canonical value-family ``TypeRecord``.
  Subsequent ``plan_for(atomic_schema)`` and ``type_for(atomic_schema)``
  calls return that canonical implementation.
- **Tuple, bundle, and fixed-size list schemas** are synthesised on
  first use. ``plan_for`` recursively resolves component schemas to
  their plans and feeds them into ``MemoryUtils::tuple_plan`` /
  ``named_tuple_plan`` / ``array_plan``. Because those builders intern
  their results, the factory's cache lines up with the global plan
  cache.
- **Container kinds (Set, Map, CyclicBuffer, Queue, dynamic List)**
  pair their schema with one of the storage shapes described under
  *Container Storage Shapes* below. ``plan_for`` synthesises the
  matching plan on first request, attaching a per-instantiation
  ``*State`` struct as the ``StoragePlan::lifecycle_context`` so the
  lifecycle hooks know how to construct the storage. ``binding_for``
  is replaced by ``type_for``, which pairs the plan with the compact
  kind-specific ops table in the common type-record registry.

The time-series layer has the matching ``TSDataPlanFactory`` for the
payload/delta component inside a time-series endpoint. Atomic TSData
planning is implemented using compact scalar value storage plus a
separate delta/tracking region. Collection-shaped TSData planning is
reserved for the slot-oriented time-series stores.

A builder may use the default factory to obtain a default plan, or
swap in an alternative factory for a specialised implementation
(e.g. the ``TSDataPlanFactory`` resolving slot-oriented time-series
data for the same logical set/map schema). The schema itself remains
untouched in both cases.

Owning Value
------------

``Value`` is the owning handle:

- ``Value(const ValueTypeMetaData&)`` — constructs an owning value for the
  given schema, in a typed-null state. Schema is preserved even when the
  payload is absent.
- ``Value(T&&)`` — convenience for scalars; resolves to the canonical
  scalar type via ``TypeRegistry::scalar_type<T>()``.
- ``has_value()`` / ``reset()`` — manage top-level payload presence.
- ``view()`` — produces a ``ValueView`` over the live payload.
- ``hash()`` / ``equals()`` / ``compare()`` / ``to_string()`` — route
  through ``view()`` and the bound ``ValueOps``. ``compare()`` returns
  ``std::partial_ordering`` to match the type-erased ``<=>``-style
  contract. ``Value`` itself only carries the minimum behaviour needed
  to live in a container.
- ``clone()`` and construction from ``ValueView`` — copy the represented
  type and payload, preserving typed-null state when the view carries
  a type record without a live payload.
- ``to_python()`` / ``from_python()`` when the Python bridge is enabled.
  ``Value::to_python()`` maps a typed-null value to ``None``;
  ``Value::from_python(None)`` calls ``reset()``. Non-null conversion
  rebuilds the owning value using the canonical scalar binding.

Anything richer than container-membership operations is exposed through
``ValueView`` or one of its specialized adapters (see *Erased Types*),
never directly on ``Value``.

Storage and Allocation
----------------------

Memory is owned by ``MemoryUtils::ErasedOwner``, parameterised by
an inline-storage policy and ``TypeRecord`` identity. Lifecycle hooks
delegated to ``LifecycleOps`` on the record's ``StoragePlan`` cover
only object lifetime:

- ``construct``, ``copy_construct``, ``move_construct``
- ``copy_assign``, ``move_assign``
- ``destroy``

Allocation is **not** part of ``LifecycleOps``. The ``ErasedOwner``
holds an allocator separately — by default a heap allocator with the
plan's alignment, but any allocator with the matching size and
alignment contract can be used — and consults the bound
``StoragePlan`` only for size and alignment when it needs to acquire
memory. Once the storage exists, the lifecycle ops construct into it;
teardown reverses the order: ``destroy`` runs first, then the
allocator releases the buffer. The ``StoragePlan`` is therefore
reusable across allocation strategies without modification.

Small payloads use inline (SBO) storage; larger payloads heap-allocate
with schema alignment. Container kinds have their own internal
storage shapes that keep element addresses stable across growth and
reconciliation; those shapes feed directly into the time-series
representation and are described under *Time-Series Plans and Ops*.
An erased owner has no borrowed state: external access is represented by a
two-word typed pointer or family view.

A scalar ``StoragePlan`` does not use a slot store. Slot stores carry
the delayed-erase and per-slot-bit machinery the time-series layer
needs to expose deltas; for scalars, that overhead is wasted — a flat
``ErasedOwner`` is sufficient. The slot store family is introduced
separately in *Time-Series Plans and Ops*.

Nullability
-----------

Null is a *state*, not a schema or type. There is no null
``ValueTypeMetaData``.

- **Top-level**: ``Value::has_value()`` distinguishes a typed-null
  ``Value`` (schema known, payload absent) from a populated one.
- **Nested**: composite kinds track per-child validity — per-field for
  tuples and bundles, per-element for fixed and dynamic lists, per-slot
  for map values. Map keys and set elements are non-null by design.

Top-level null Values map to Python ``None`` at the bridge boundary;
``Value::from_python(None)`` calls ``reset()``. A non-owning
``ValueView`` cannot reset its source storage, so
``ValueView::from_python(None)`` is rejected; use the owning ``Value``
when top-level null assignment is required.

Container Storage Shapes
------------------------

The value layer's container kinds (List, Set, Map, CyclicBuffer,
Queue) are deliberately **compact** and **immutable after
construction**. At the scalar layer, a value is set atomically — read
whole, replaced whole, hashed and compared whole — and never mutated
piecemeal through the typed view.

The default storage choice follows what is known from the schema:

- A ``List`` schema with ``fixed_size > 0`` uses
  ``MemoryUtils::array_plan``. The element count is part of the schema
  and the default payload can be constructed directly in the fixed
  array layout.
- A ``List`` schema with ``fixed_size = 0`` uses ``ListStorage``. The
  actual element count is known at construction time and remains fixed
  for the lifetime of that value.
- ``CyclicBuffer`` and ``Queue`` are the same: their length is fixed
  at construction and cannot change. The ring / FIFO interpretation
  is purely a read-time concern (where the head sits, what order
  ``front()`` walks).
- ``Set`` and ``Map`` are populated once at construction; lookup and
  iteration are the only post-construction operations.

The time-series layer is what changes the picture: there, per-element
insert / remove across ticks is meaningful, the storage is slot-store-
based, and the fixed-vs-dynamic distinction tracks how the slot store
is allowed to grow (see *Time-Series Plans and Ops > The Slot Store
Family*). At the value layer, those concerns do not apply.

The compact storage shapes are:

``ListStorage``
    Sized contiguous buffer of element-plan-shaped slots for dynamic
    list schemas. Holds exactly the number of elements supplied at
    construction; cannot be resized; individual elements cannot be
    replaced. Fixed-size list schemas use ``MemoryUtils::array_plan``
    instead.

``SetStorage``
    Content-keyed hash set populated once at construction. Elements
    live in a contiguous buffer and lookup uses an
    ``ankerl::unordered_dense`` slot index that stores element-slot
    ids, not copied keys. Hashing and equality come from the bound
    element ops. Direct construction rejects duplicate keys; the
    builder deduplicates before storage is built. ``contains`` is
    required to be average O(1); semantic equality for two different
    set representations depends on iterating one side and performing
    one lookup per element on the other side. Set comparison orders by
    size when sizes differ; same-sized sets compare equivalent when
    their members match and unordered otherwise.

``MapStorage``
    Content-keyed hash map populated once at construction. Keys and
    values live in parallel contiguous buffers and lookup uses the
    same ``ankerl::unordered_dense`` slot-index pattern over the key
    buffer. Direct construction rejects duplicate keys; the builder
    overwrites the value for an existing key before storage is built.
    ``contains`` and ``value_at`` are required to be average O(1);
    semantic equality for two different map representations depends
    on iterating one side and performing one key lookup per entry on
    the other side.

``CyclicBufferStorage``
    Sized contiguous buffer with a logical *head* offset that
    interprets the buffer as a ring. Immutable once constructed;
    the read view walks elements in ring order
    (``head``, ``head+1``, …, wrapping). The slot-store-based
    time-series variant supplies the rolling-overwrite semantics.

``QueueStorage``
    Sized contiguous buffer holding the queued elements in arrival
    order. Immutable once constructed; the read view exposes
    ordered iteration and ``front()``. The slot-store-based time-
    series variant supplies push / pop semantics over time.

Mutation. The compact value-layer storage shapes do **not** expose
per-element structural mutation. Their ops tables set
``allows_mutation = false``, so ``begin_mutation()`` fails even if the
erased view was created from writable storage. Whole-container
replacement happens at the ``Value`` level via copy / move assignment
or ``from_python()`` — the bound plan's lifecycle ops swap one storage
for another. Atomic scalar assignment is still explicit: open a
mutable view with ``begin_mutation()`` and then call
``ValueView::set<T>`` or ``checked_mutable_as<T>()``. Structural
per-element insert / remove / resize methods only make sense for the
slot-store-based time-series variants and will live on the
``MutableListView`` / ``MutableSetView`` / ``MutableMapView`` family,
not on compact scalar container storage.

Lifecycle context. Each storage shape pairs with a small ``*State``
struct (``ListState``, ``SetState``, ``MapState``,
``CyclicBufferState``, ``QueueState``) carried as the
``StoragePlan::lifecycle_context``. The state holds the ``ValueTypeRef`` values
and any per-instantiation parameters (constructed size, ring head)
so the plan's ``default_construct`` and copy/move hooks can
assemble the storage without knowing the C++ template arguments at
the call site. The state structs are owned by the plan registry
alongside the interned plan and live as long as it does.

Building containers
-------------------

Constructing a compact, immutable container requires *all* of its
elements to be available at the moment ``default_construct`` /
``copy_construct`` runs. For statically known data that is fine:
the caller assembles a temporary buffer of elements and hands it to
the construction site. For data accumulated piecemeal (e.g.
generated by a graph wiring step that walks a set of inputs), the
copy-once-finalised pattern is awkward.

The implementation uses a **value builder** — a per-kind builder type
that is mutable while it is being filled and produces an immutable
``Value`` on ``build()``:

- ``ListBuilder``    — ``push_back(element)`` / ``size`` / ``build``.
- ``SetBuilder``     — ``insert(key)`` / ``build``.
- ``MapBuilder``     — ``set_item(key, value)`` / ``build``.
- ``CyclicBufferBuilder`` — ``push_back(element)`` (overwrites
  oldest once the declared capacity is reached) / ``build``.
- ``QueueBuilder``   — ``push(element)`` / ``build``.

The builder owns growable scratch storage during accumulation
(``std::vector`` / hash table); on ``build()`` it copies — or, where
safe, moves — the accumulated elements into a freshly-constructed
compact ``*Storage`` of the now-known size. The resulting ``Value``
is immutable, matching the design contract above; the builder
itself is single-use and local to the construction site.

This is deliberately different from the reusable builders used above
the value layer. A value builder represents the *one value currently
being assembled*. It should not be interned or cached as a factory for
repeatedly recreating the same value. If the same container value is
needed again, construct a new value builder or copy the resulting
``Value`` according to normal value semantics.

This keeps compact container storage immutable after construction
while still letting callers build a value in stages.

Status. The compact value-layer storage shapes, builders, specialised
read-only views, and ``ValuePlanFactory`` compact container types
are implemented. The slot-store-based shapes used by the time-series layer
*are* described under *Time-Series Plans and Ops > The Slot Store
Family*; they are not the value layer's compact form and should not be
reused as such.

Buffer and Arrow Interop
------------------------

Exposing value data as a buffer is a first-class concern of the value
layer. Internal storage is not required to be Arrow-native, but every
kind whose payload can be sensibly viewed as a contiguous buffer must
expose a path to do so:

- atomic and fixed-size composite kinds expose a direct buffer view
  over their payload memory.
- dynamic list and queue kinds expose buffer views over their element
  storage, with a separate validity buffer where nested nullability
  applies.
- map and set kinds expose key, value, and validity buffers in
  parallel.

Validity bits follow Arrow conventions (``1 = valid``, ``0 = null``)
so a buffer view is interchangeable with an Arrow-compatible consumer
without a translation step. This applies whether the buffer is
exported for analytics, sent over an external bridge, or consumed by
an Arrow-aware adaptor.

Buffer access is read-only at the value layer. Mutation continues to
go through the typed view APIs.
