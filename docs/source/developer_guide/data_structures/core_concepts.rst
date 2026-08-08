Core Concepts
=============

Every data structure in the runtime is built from the same six-element
vocabulary. Defining it once lets each layer below describe itself by naming
the concrete Plan, Schema, Ops, and Builder it uses, instead of restating the
pattern.

The vocabulary is grouped into three roles: a *concept* group that describes
what something is, a *resolution* group that picks an implementation for it,
and a *data* group that holds and exposes the actual instance.

.. mermaid::

   flowchart LR
      subgraph interned["Interned — stable addresses, program lifetime"]
         Schema["Schema<br/>(layout-free type identity)"]
         Plan["Plan<br/>(size/alignment/offsets + lifecycle ops)"]
         Ops["Ops<br/>(struct of fn-ptrs)"]
      end
      Builder["Builder<br/>(the only place Schema binds to a Plan + Ops)"]
      Value["Value<br/>(owns memory; constructed in place by the Plan)"]
      View["View<br/>(borrows memory + Ops)"]

      Schema --> Builder
      Plan --> Builder
      Ops --> Builder
      Builder -->|"constructs into pre-allocated storage"| Value
      Value -->|"borrow"| View

Concept Group
~~~~~~~~~~~~~

``Plan``
    Memory-layout primitive. A Plan describes the size, alignment, and
    field layout for one data structure, and pairs that with the
    construction, copy / move, and destruction operations needed to
    bring an instance to life in already-allocated memory. A Plan does
    **not** allocate or deallocate memory and carries no reference to
    any allocator; allocators consume a Plan's size and alignment
    separately to acquire storage. A Plan does not carry semantic
    meaning beyond memory layout and lifecycle.

``Schema``
    Independent, generic description of a concept. A Schema describes what a
    time-series, node, or higher-level structure *is*. It is not tied to any
    particular memory layout or behaviour, so multiple implementations of the
    same concept share one Schema.

``Ops``
    Struct of function pointers that exposes behaviour for a data structure.
    The first parameter of every operation is always a pointer to the memory
    representing the structure; remaining parameters follow as needed. An
    Ops table is the type-erasure vehicle for one implementation of a
    concept.

Plan, Schema, and Ops are plain C++ structs and are immutable once
constructed.

Resolution Group
~~~~~~~~~~~~~~~~

``Builder``
    Construction role for a concept. A Builder resolves, or is given, the
    Schema / Plan / Ops / Binding needed to construct a concrete runtime
    object. The lifetime of a Builder depends on which layer it belongs to:

    - A **reusable builder** is a cached blueprint. It binds the resolved
      implementation once and can construct multiple runtime instances.
      Time-series value builders, node builders, and graph builders are in
      this category. Graph builders also pull double duty for nested graphs:
      the parent graph can retain the child graph builder as the reusable
      template for each nested instance.
    - A **value builder** is an instance assembler. It owns mutable scratch
      storage while a specific scalar/value-layer value is being assembled,
      then produces one ``Value`` on ``build()``. It is not a canonical
      implementation object and is not intended to be cached for repeated
      construction of the same value.

    A Builder may itself be type-erased so that generic construction code can
    hold builders for unrelated concepts uniformly.

Anything that needs a runtime object for a given Schema goes through the
appropriate Builder category for that layer.

Data Group
~~~~~~~~~~

``Value``
    Holds the data described by a Plan. By definition, Values are
    type-erased storage. A Value carries only the minimum behaviour needed
    to live in a container: destruction, copy and move construction and
    assignment, equality, and hash. Reads, writes, comparisons over content,
    and iteration are exposed through a View, never directly on the Value.
    A Value owns its allocator separately from its Plan: the Plan
    contributes size and alignment, the allocator owns the storage,
    and the Plan's lifecycle ops construct and destruct into that
    storage.

``View``
    Constructed from an existing Value. A View references the Value's
    memory and pairs it with the corresponding Ops table so that behaviour
    can be invoked. A View carries no payload of its own; it is a
    lightweight reference.

Interning
~~~~~~~~~

Plans, Schemas, Ops tables, Bindings, and reusable Builders are *interned*:
there is only ever one true instance of each, kept alive by an intern table
that returns stable references for structurally equivalent inputs. Any
consumer—a View, a Builder, a node, a graph—holds a borrowed pointer to its
Plan, Schema, Ops, Binding, or reusable Builder for the artifact's whole
lifetime without managing ownership.

The generic vehicle is ``InternTable<Key, Value>``, which guarantees stable
addresses for the values it owns.

Values and value builders are *not* interned. Each ``Value`` instance is
independent, and each value builder is local scratch state for one build.

The Universal Pattern
~~~~~~~~~~~~~~~~~~~~~

Every layer below instantiates the same shape:

.. mermaid::

   flowchart LR
      Schema[Schema<br/>concept]
      Plan[Plan<br/>memory layout]
      Ops[Ops<br/>function pointers]
      Builder[Builder<br/>resolved blueprint or instance assembler]
      Value[Runtime instance<br/>Value / time-series / node / graph]
      View[View<br/>exposes behaviour]

      Schema --> Builder
      Plan --> Builder
      Ops --> Builder
      Builder -->|constructs| Value
      Value -.referenced by.-> View
      Ops -.bound into.-> View

A Builder is fed by a Schema (what it is), a Plan (how its memory is laid
out), and an Ops table (how it behaves). Reusable builders cache that
resolved construction recipe and construct many instances; value builders
hold one in-progress value's scratch data and are consumed by ``build()``.
A View is built from an instance and pulls in the same Ops table so that
callers can act on the instance's data where that layer exposes views.

Subsequent sections name the concrete Schema, Plan, Ops, and Builder used
for each layer rather than re-introduce the pattern.

Bundle field validity (ruling 2026-07-06)
-----------------------------------------

A ``TSB`` is a collection of time-series; its value and delta value are
collections of the FIELDS' values / delta values. Both conversions must
represent **not-set fields**: a field that fails its filter (not valid /
not modified this cycle) is UNSET — it must never surface as a default
value (the derived schema has no basis for one).

Mechanism: Bundle **plans** carry a hidden trailing unnamed validity
component: a fixed array of ``uint64_t`` words with one bit per field. The
SCHEMA is unchanged — validity is a storage concern, exactly like Arrow
validity buffers — and the auxiliary component is not visible through
field lookup:

- ``BundleBuilder::set`` marks the field valid; unset fields stay
  invalid after ``build()`` (default construction is all-unset).
- Field access on an unset field yields an EMPTY ``ValueView``
  (``has_value() == false``).
- ``equals``/``compare``/``hash``/``to_string`` respect the bits
  (unset == unset; unset < set).
- Conversions (Python dicts, JSON, tables) omit / null unset fields;
  building from a dict marks exactly the provided keys.
- ``capture_delta`` over TSB sets only modified+valid children;
  ``apply_delta`` / ``delta_has_effect`` skip unset fields.

**Fixed ``Tuple`` composites carry the same hidden validity words**
(ruling 2026-07-08): a ``combine[TS[Tuple]](a, b, __strict__=False)``
must be able to emit a partial tuple whose unfilled slot reads back as
``None``, so fixed tuples now allocate the same trailing validity
component as bundles. ``BundleBuilder`` marks tuple slots on ``set`` (it
no longer short-circuits on non-bundle kinds); a sequence/dict fill
marks every supplied slot; ``equals``/``compare``/``to_python`` honour
the bits (unset tuple slot → ``None``).

**Nullable tuples, two mechanisms by size** (ruling 2026-07-09): the
nullable trait (``ValueTypeFlags::Nullable`` / ``is_nullable()``, the
``registry.nullable_tuple(element)`` factory verb) yields a tuple that
accepts holes.

- KNOWN size (fixed tuple, ``TSL[..,Size[N]]`` → tuple): the
  PRE-ALLOCATED trailing validity words above.
- UNKNOWN size (variadic ``tuple[X, ...]`` = the dynamic ``List``
  value): a ``sul``-style element-validity bitset ON THE COMPACT
  ``ListStorage`` (``std::vector<bool>``, EMPTY means dense/all-set, so
  non-nullable lists keep zero overhead). ``ListBuilder::push_back_
  unset`` appends a hole; the compact-list ops
  (``equals``/``compare``/``hash``/``to_string``/``to_python``) honour
  ``element_set`` (unset element → ``None``; the buffer fast-path is
  disabled when any hole exists). This is the same EMPTY-means-dense
  convention the mutable list storage uses.

``TSL`` carries the same
CONTRACT: its delta (a sparse ``Map<index, delta>``) already expresses
partial ticks; List VALUES carry the same treatment where needed:
MUTABLE list storage now has element validity (a ``sul`` bitset with
the same EMPTY-means-dense convention; ``push_back_unset`` appends a
hole, reads report the element unset, equals/compare/hash/to_string
honour the bits) - the dense recording buffer is its first consumer
(``List<delta_schema>`` with holes as unset elements, replacing
Any-boxed padding). Compact/fixed list validity remains deferred until
a use arises.

Audited value surfaces (2026-07-06 sweep — every place a composite value
is ASSEMBLED or APPLIED honours the filter):

- Non-peered INPUT endpoints (``input_value_element_at``): both
  target-link children (``has_current_value`` on the resolved target —
  note ``TSDataView::valid()`` means *has storage*, not TS-validity) and
  direct children read UNSET when invalid.
- ``apply_current_value`` over TSB and TSL: UNSET source children are
  skipped (the dense whole-plan fast path is gone for TSB).
- Fixed-structured TSData whole-value copy/move: UNSET source fields
  skip (with region-bitset marking on child writes).
- ``switch_`` mixed value/REF branches: value terminals forward into a
  dedicated value backing; the switch output publishes a peered
  reference to it; branch teardown publishes EMPTY and destroys the
  backing (consumers observe the reset — stale values must not survive
  a reload).
