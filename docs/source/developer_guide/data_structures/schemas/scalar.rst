Scalar Schemas
==============

A scalar schema is the value-layer's concept identity for a non-time-
series payload. Scalar schemas are represented at runtime by
``ValueTypeMetaData``. The struct carries:

- ``kind`` — the value kind (one of the nine listed below),
- capability flags — for example, whether the value is hashable,
  comparable, or buffer-exposable,
- component fields — for composite kinds, the field list (bundle/tuple)
  or the element/key type pointers (list/set/map),
- the canonical name — used for Python-bridge interop and diagnostic
  output.

There are no layout fields on the schema. Size, alignment, and field
offsets live on the matching ``StoragePlan``. That separation lets two
implementations of the same kind share one schema while keeping their
own storage strategies (see *Allocation, Plans and Ops > Erased
Types* — the multi-implementation rationale).

Value Kinds
-----------

A scalar schema has one of nine kinds, recorded on its
``ValueTypeMetaData``:

``Atomic``
    A single scalar: integer, floating-point, boolean, string, or one of the
    date/time scalars.

    **Enums** are named atomic scalars (ruling 2026-07-10: a first-class
    value kind, never a python-object ride-along). An enum schema is
    interned NOMINALLY by its type name — like a named bundle — and
    carries its ordered member table on the schema's ``fields`` array
    (member name plus the member's ASSIGNED integer value; the
    ``ValueTypeFlags::Enum`` trait marks the meta). The stored payload is
    the assigned integer value itself, so the generic comparison ops
    order enums exactly as hgraph does (`.value` comparison) with no
    enum-specific kernels; ``to_string`` renders the member NAME, and
    the python conversion maps to the REGISTERED python ``Enum`` class
    (the bridge registers each class against its interned meta — the
    ``CmpResult``/``DivideByZero`` slot mechanism generalised).
    Re-registering the same name requires an identical member table.

``Tuple``
    Fixed-arity ordered fields, accessed by index. Field types may differ.

``Bundle``
    Named tuple. Fields are accessed by name; field-name metadata is
    preserved in the storage plan.

    A bundle is either **un-named** or **named**. The schema content
    (the ordered ``(field_name, field_type)`` list) is stored on the
    un-named form; a named bundle layers a name on top of it and
    holds a borrowed pointer to the un-named schema. The two forms
    have distinct identity semantics:

    - Two un-named bundles with the same field list are the **same
      schema** (structural equality, single interned pointer).
    - Two named bundles with the same field list but different names
      are **distinct schemas** (nominal equality — name is part of
      identity).
    - A named bundle can be assigned values from an un-named bundle
      with the same field list, since the named form's structural
      content is exactly the un-named schema it wraps. The reverse
      direction is also allowed; only named ↔ named with different
      names is rejected.

    This split exists so structural shapes used by the runtime
    (delta bundles, tuple-of-fields algebra) can stay anonymous and
    de-duplicate freely, while user-defined records (``Trade``,
    ``Quote``) keep nominal identity even when their fields happen
    to coincide.

    *Python correspondence.* Both native ``CompoundScalar`` classes and
    Python-owned structured scalars map onto the C++ ``Bundle`` kind. A
    dataclass subclass of ``CompoundScalar`` is a qualified nominal Bundle
    with offset-backed native storage; its default namespace is the defining
    module plus enclosing scope.
    ``class Quote(CompoundScalar, namespace="feed", abstract=True,
    discriminator="kind")`` overrides the namespace, construction policy,
    and external type marker. Anonymous compounds produced by
    ``compound_scalar(**kwargs)`` remain structural.

    A plain standard-library dataclass, or an explicitly registered annotated
    Python class, is also a qualified nominal Bundle but uses a Python-owned
    binding. That binding retains the exact object and implements complete
    read-only ``IndexedValueOps`` by lazy attribute projection. Consequently
    ``ValueView::as_bundle()`` returns the same representation-erased
    ``BundleView`` for either storage policy. Generic code must select by
    Bundle/indexed capability and must not assume field offsets merely because
    the schema kind is ``Bundle``.

    Named Bundle metadata records immediate parents and children. Multiple
    inheritance is allowed, but a child must contain every inherited field
    with exactly the inherited type. A child may add fields or restate an
    inherited field identically; it may not change one. Generic
    specializations are invariant, carry their argument schemas, and have
    distinct nominal identities (for example ``Box[int]`` and ``Box[str]``).

Closed Bundle unions
--------------------

``Wiring::finish`` captures the registered Bundle hierarchy in an immutable
``TypeRealizationSnapshot``. A later subclass registration does not resize or
change an already-wired graph. Materialising a Python base recursively
registers its concrete, already-defined ``__subclasses__`` before the snapshot
is captured. Classes imported or defined only after wiring are intentionally
outside that graph's closure; concrete generic specializations must also be
used or inherited concretely before wiring completes.

When a declared Bundle had children at capture time, ``TS[Base]`` uses a
closed-union value plan. Its storage is one ``TypeRecord*`` followed by an
aligned payload large enough for the largest concrete alternative in the
captured closure. The pointer identifies the active concrete schema and the
payload is constructed in place. Equality and hashing include that schema;
ordering is lexicographic only within the same concrete schema and unordered
between siblings. Abstract Bundles are excluded as alternatives. A
non-abstract parent remains directly constructible.

A declared leaf uses its ordinary canonical Bundle plan. It has no union
header, padding, or other polymorphism overhead, including when it is itself a
descendant. If one of the leaf's fields is itself a polymorphic Bundle, only
that field uses its realized union plan; the containing leaf does not acquire a
second type header. Nested graphs inherit the root graph's snapshot during
dynamic construction, so they cannot observe a different closure. Registry
snapshots copy only the type order and immediate inheritance edges; transitive
closures and realized plans are built lazily for schemas a graph actually uses.
Dynamic immutable and mutable lists, sets, maps, cyclic buffers, and queues
retain their existing storage shapes while carrying realized element bindings.
Fixed lists currently reject polymorphic Bundle elements at wiring instead of
erasing the concrete type.

Python conversion constructs the active concrete dataclass directly in the
union. ``None`` means an unset Bundle field. A dictionary or JSON object being
decoded *through a polymorphic parent* must include the configured
discriminator (``__type__`` by default), naming a valid qualified or
unambiguous local alternative. The marker is an external wire artifact and is
not stored as a Bundle field.

Recursive Bundle fields
-----------------------

A recursive field is represented by an ``Owned[T]`` value schema. Its inline
storage plan is exactly one owner pointer, independent of ``T``. ``None`` or
an unset field leaves that pointer null. Mutable access, Python conversion, or
JSON decoding allocates the pointee on demand; copy operations deep-copy it.
The allocated block retains the pointee's ``TypeRecord`` before its aligned
payload, so a graph-scoped polymorphic pointee is destroyed correctly even
after the thread-local realization scope has ended.

C++ code can request an owner with ``TypeRegistry::owned(target)``. A
self-recursive nominal schema is declared atomically with
``TypeRegistry::recursive_bundle(...)``; a null field-schema entry denotes
``Owned<Self>``. Python recognises direct self references, including
``Optional[Self]``, on a dataclass ``CompoundScalar`` and uses the same native
owned path. Python-owned dataclasses may also describe direct or mutual
recursion because their annotations do not imply recursive inline native
storage; their bridge registration resolves the class graph before publishing
the concrete bindings. A recursive native ``CompoundScalar`` still requires
the ``Owned[T]`` representation described above.

``List``
    Ordered sequence of one element type. May be ``fixed_size`` or dynamic.
    A dynamic list is **immutable** (built once) by default; a list schema
    carrying ``ValueTypeFlags::Mutable`` (from ``TypeRegistry::mutable_list``)
    is instead **structurally mutable** — backed by growable slot-store
    storage and supporting ``push_back`` / ``set`` / ``erase`` / ``pop_back``
    / ``clear`` through ``MutableListView``. Mutability is an explicit schema
    axis, so the two forms intern separately and never collide.

``Set``
    Unordered collection of unique elements of one type. Immutable (built once)
    by default; a set schema carrying ``ValueTypeFlags::Mutable`` (from
    ``TypeRegistry::mutable_set``) is **structurally mutable** — backed by a
    ``KeySlotStore`` and supporting ``add`` / ``remove`` / ``clear`` through
    ``MutableSetView``, with order-independent hash-based equality. Mutability is
    an explicit schema axis that interns separately. A mutable set is the field
    type of a ``TSS`` delta bundle in the testing toolkit.

``Map``
    Key/value mapping with one key type and one value type. Immutable
    (built once) by default; a map schema carrying ``ValueTypeFlags::Mutable``
    (from ``TypeRegistry::mutable_map``) is **structurally mutable** — backed
    by a ``KeySlotStore`` + co-indexed ``ValueSlotStore`` and supporting
    ``set_item`` / ``remove`` / ``clear`` through ``MutableMapView``. As with
    the list, mutability is an explicit schema axis that interns separately.
    A mutable ``Map<string, Any>`` is the backing for the runtime
    ``GlobalState`` injectable; an **immutable** ``Map<int, T>`` (built once
    via ``MapBuilder``) is the delta value of a ``TSL`` time-series in the
    testing toolkit. Map **values** support element validity: an entry may
    carry an UNSET value (``MapBuilder::set_item_unset``) — Python's
    ``None``-valued mapping entry, JSON ``null`` — read back as an absent
    value view / ``None``; keys are never nullable.

``CyclicBuffer``
    Fixed-capacity ring buffer of one element type.

``Queue``
    FIFO queue with capacity and ordering.

``Any``
    A type-erased "any value" box. **Compile-time schema knowledge ends
    here**: an ``Any`` carries no element/key/field references — it is
    unconstrained — and its storage is an embedded owning ``Value`` whose
    own schema is only known at run time. The box is empty until a value is
    assigned; assigning copies the value in, after which the contained
    ``Value`` carries its own schema and owns its own memory. ``Any`` is the
    singleton schema returned by ``TypeRegistry::any()``.

    ``Any`` is the slot type that lets value-layer containers hold
    heterogeneous contents (a ``List<Any>`` or ``Map<K, Any>`` where each
    element may have a different concrete schema), and is the eventual
    analogue of a generic / Python ``object``. ``hash`` / ``equals`` /
    ``compare`` / ``to_string`` delegate to the contained value, with an
    explicit empty state (empty equals empty; empty orders before any
    non-empty value; an empty box stringifies as ``"None"``).

Each kind has a corresponding specialised view at the runtime layer
(``TupleView``, ``BundleView``, ``ListView``, ``SetView``, ``MapView``,
``CyclicBufferView``, ``QueueView``, ``AnyView`` / ``MutableAnyView``);
those views are described under *Allocation, Plans and Ops > Erased Types*.

Composite Schema Synthesis
--------------------------

Composite schemas are synthesised on first use. ``TypeRegistry``
exposes typed factory methods — ``tuple(...)``, ``un_named_bundle(...)``,
``bundle(...)``, ``list(...)``, ``set(...)``, ``map(...)``,
``cyclic_buffer(...)``, ``queue(...)`` — each backed by an internal
``InternTable<TypedKey, ValueTypeMetaData>`` keyed by structural
inputs. Calling ``map(string_meta, int_meta)`` twice returns the
same pointer; the second call resolves directly off the intern
table.

The bundle pair reflects the named/un-named distinction described
under *Value Kinds*:

``un_named_bundle({(name, type)...})``
    Interns by the structural key (the ordered field list). Returns
    the canonical un-named schema for that field list.

``bundle(name, {(name, type)...})``
    Requires a name. Internally synthesises the un-named bundle for
    the field list, then interns a named wrapper keyed by
    ``(name, un_named_pointer)``. The named schema records its name
    and a borrowed pointer to the un-named schema rather than a
    second copy of the field list, so the field-list content lives
    in exactly one place.

The typed keys are deliberately structural rather than string-based.
For example, the map registry keys on ``MapKey{key_meta_ptr,
value_meta_ptr}`` rather than a synthesised ``"Map<string,int>"``
name. This keeps key construction cheap and avoids stringification
mismatches across composition orders.

The typed keys are deliberately structural rather than string-based.
For example, the map registry keys on ``MapKey{key_meta_ptr,
value_meta_ptr}`` rather than a synthesised ``"Map<string,int>"`` name.
This keeps key construction cheap and avoids stringification mismatches
across composition orders.
