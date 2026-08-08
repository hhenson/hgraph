Time-Series Schemas
===================

A time-series schema describes the runtime-side wrapper that lets a
value participate in graph evaluation. It carries the universal
time-series contract — modification time, validity, delta tracking — on
top of a value-layer payload. The schema is represented at runtime by
``TSValueTypeMetaData``; the actual memory layout and storage strategy
live on the matching plan and are described in
:doc:`../plans_and_ops/time_series`.

``TSValueTypeMetaData`` is a standard-layout unified schema. Its first
field, at offset zero, is ``SchemaHeader``; it does not inherit from the
older ``TypeMetaData`` base. The header owns the immutable canonical
diagnostic label and records ``TypeFamily::TimeSeries`` plus the numeric
kind ABI. The kind values are fixed as ``TS=0``, ``TSS=1``, ``TSD=2``,
``TSL=3``, ``TSW=4``, ``TSB=5``, ``REF=6``, and ``SIGNAL=7``. Named and
structural TSB identity remains represented by ``wrapped_un_named``;
both forms still have valid, non-empty header labels.

This chapter describes:

- the eight time-series kinds and what they wrap,
- the tick-level contract every kind exposes,
- the per-element modification model,
- the schema-level pre-computation of ``value_schema`` and
  ``delta_value_schema`` for each kind.

Time-Series Kinds
-----------------

``TS``
    Scalar time-series of one atomic type.

``TSB``
    Bundle of time-series fields. Like the value-layer ``Bundle``, a
    ``TSB`` is either **un-named** or **named** (see *Scalar Schemas
    > Bundle* for the structural-vs-nominal identity rule). The
    schema content (the ordered ``(field_name, ts_schema)`` list) is
    stored on the un-named form; a named TSB layers a name on top
    and holds a borrowed pointer to the un-named schema. Two un-
    named TSBs with the same field list are the same schema; two
    named TSBs with the same field list but different names are
    distinct schemas. A named TSB can be assigned values from an
    un-named TSB with the same field list (and vice versa); named ↔
    named with different names is rejected.

    The companion factory pair on ``TypeRegistry`` is therefore
    ``un_named_tsb({...})`` and ``tsb(name, {...})``, mirroring the
    bundle pair.

    *Python correspondence.* Python ``TimeSeriesSchema``
    (``hgraph._types._tsb_type.TimeSeriesSchema``) maps onto the
    C++ ``TSB`` kind. A subclass of ``TimeSeriesSchema`` is the
    *named* TSB whose name is the Python class name. An installed
    native extension may declare
    ``class Output(TimeSeriesSchema, namespace="example.extension")``;
    its TSB name is then ``example.extension::Output``, allowing the
    Python authoring schema and a public C++ named TSB to share exact
    nominal identity without lifting a compound scalar as a proxy.
    ``UnNamedTimeSeriesSchema.create(**fields)`` (also exposed as
    the convenience ``ts_schema(**fields)``) is the *un-named*
    form. The runtime ``TimeSeriesBundle`` instance is the C++
    ``TSB`` value. ``TSB[schema].value`` in Python is a
    ``CompoundScalar`` — the snapshot of currently valid fields —
    which lines up with the schema-mapping table below: ``TSB``'s
    ``value_schema`` is a ``Bundle`` whose fields are the per-
    field ``value_schema`` of each TSB component, and that
    ``Bundle`` is exactly the C++ representation of the
    corresponding Python ``CompoundScalar``.

``TSL``
    Ordered list of one time-series type, fixed-size or dynamic.

``TSS``
    Unordered set of one scalar type.

``TSD``
    Keyed dictionary with scalar keys and time-series values.

``TSW``
    Sliding window of one scalar type.

``REF``
    Reference to a time-series target. The target is resolved through
    binding state rather than carried inline.

``SIGNAL``
    Boolean tick — used purely as a notification primitive.

Tick Semantics
--------------

Every time-series, regardless of kind, exposes the same five tick-level
properties. These are the universal time-series contract. Inputs and
outputs both expose them; on an input, the values reflect the bound
output. The kind-specific view APIs add container-specific access on
top of this base, but the base is always present.

``value``
    The cumulative current state of the time-series. For a ``TS`` this
    is the scalar that has been written; for a ``TSB`` it is the bundle
    of all currently valid fields; for a ``TSD`` it is the map of all
    live keys to their current values. ``value`` persists across ticks
    until it is replaced or extended.

``delta_value``
    What changed in the current tick. ``delta_value`` is ``None`` when
    the time-series did not tick in the current evaluation cycle. When it
    did tick, the shape depends on the kind:

    - ``TS`` — ticks are atomic value replacements with no diffing,
      so ``delta_value`` is the new scalar (the same object as
      ``value``). Setting a ``TS`` to its current value still
      produces a delta.
    - ``TSB`` — modified fields only.
    - ``TSL`` — modified elements with their indices.
    - ``TSS`` — added and removed sets.
    - ``TSD`` — removed keys and per-key modified values. Added and
      updated keys and values both appear in the delta view api, as do removed values.
      The delta value is a compacted version of the information and is sufficient to
      recreate the changed state using repeated applications of the delta value to an
      output TSD.
    - ``TSW`` — the element added in the current tick (if any).

    ``delta_value`` is logically present only for the evaluation cycle of
    the change. The runtime is not required to physically reset it at
    the end of the cycle, and in fact prefers lazy cleanup: removed
    ``TSS`` and ``TSD`` slot entries live past their logical destruction
    so the current cycle's delta can be read, and physical erase happens
    later at the next outermost ``begin_mutation()``. In a subsequent
    evaluation cycle, querying ``delta_value`` returns ``None`` if no
    new change has occurred, or the new change if one has.

    The expectation of a delta value is that repeated applications of a delta value
    on an output should cause the output to exactly represent the input state over time.

``modified``
    Whether the time-series ticked in the current evaluation cycle. For
    container kinds this is recursive: a container is modified if any
    of its elements is modified. Equivalent to
    ``last_modified_time == evaluation_time``. Must be an O(1) query —
    consumers test it on every active input on every tick, so it is
    read directly from per-TS state, never derived by scanning children.

``valid``
    Whether the time-series has ever been ticked. Equivalent to
    ``last_modified_time != MIN_DT`` (``MIN_DT`` is the sentinel for
    never-modified). Note that ``valid`` does *not* imply the
    time-series holds any content: a ``TSS`` or ``TSD`` that ticked
    with no members is valid but empty. Also an O(1) query.

``last_modified_time``
    The evaluation time at which the time-series was last modified. Stored
    once per time-series instance in the per-TS runtime state tree.
    Defaults to ``MIN_DT`` until the first tick.

Per-Element Modification
~~~~~~~~~~~~~~~~~~~~~~~~

For composite kinds, a consumer often needs to ask not just *did this
container tick?* but *which children ticked?*. The answer comes from
each child's own time-series state: every time-series instance — at
every level of nesting — carries its own ``last_modified_time`` as
part of the per-TS runtime state tree. A collection like TSD holds a
vector of slots where each slot contains the full time-series state of
that element, including the child's ``last_modified_time``. Walking
the slots and inspecting each child's ``last_modified_time`` is how
the container reports which elements changed in the current cycle —
there is no separate side structure tracking this.

The container's own ``last_modified_time`` is updated whenever any
child ticks, which is what makes the recursive container-level
``modified`` query a single field read rather than a tree walk. A
slot id is therefore both a path identifier and the key into the
child's full time-series state.

Value and Delta-Value Schemas
-----------------------------

The conceptual ``value`` and ``delta_value`` shapes described under
Tick Semantics surface as two pre-computed properties on every TS
schema (``TSValueTypeMetaData``):

- ``value_schema`` — the value-layer schema of the time-series's
  runtime ``value``.
- ``delta_value_schema`` — the value-layer schema of its
  ``delta_value`` (the per-tick change set).

Both are populated during schema registration and read as plain field
accesses — consumers never need to recompute them. The same composite
schemas the value layer interns back the underlying storage, so two TS
schemas with the same value/delta shapes share the same value-layer
schema pointers.

Per-kind mapping:

.. list-table::
   :header-rows: 1
   :widths: 20 35 45

   * - Kind
     - ``value_schema``
     - ``delta_value_schema``
   * - ``TS<T>``
     - ``T``
     - ``T``
   * - ``TSS<T>``
     - ``Set<T>``
     - ``Bundle{added: Set<T>, removed: Set<T>}``
   * - ``TSD<K, V>``
     - ``Map<K, V.value_schema>``
     - ``Bundle{removed: Set<K>, modified: Map<K, V.delta>}``
   * - ``TSL<T>``
     - ``List<T.value_schema, fixed_size>``
     - ``Map<int, T.delta_value_schema>``
   * - ``TSW<T>`` (tick)
     - ``List<T, period>``
     - ``T``
   * - ``TSW<T>`` (duration)
     - ``List<T, 0>`` (dynamic)
     - ``T``
   * - ``TSB{f...}``
     - ``Bundle{f: f.value_schema...}``
     - ``Bundle{f: f.delta_value_schema...}``
   * - ``REF<T>``
     - ``TimeSeriesReference``
     - ``TimeSeriesReference``
   * - ``SIGNAL``
     - ``bool``
     - ``bool``

A few notes on the cells that aren't immediate:

- ``REF<T>`` is conceptually ``TS<TimeSeriesReference>``: the reference
  token itself is the value, and dereferencing to read the target's
  value is a runtime concern handled through binding state, not
  through ``value_schema`` or ``delta_value_schema``. A reference token
  is either empty, peered to a single ``TSOutputHandle``, or a
  non-peered collection of child reference tokens. Input views can
  produce reference tokens for any shape: target links become direct
  output references when bound and typed empty references when unbound;
  non-peered structural prefixes recursively collect child reference
  tokens; leaf shapes reached without a target link become typed empty
  references. Note the
  asymmetry: every ``REF<T>`` *schema* (the ``TSValueTypeMetaData``)
  is unique per ``T`` and continues to carry the wrapped target
  schema via ``referenced_ts()`` — useful for binding validation,
  introspection, and ``dereference()``. What is shared across all
  ``REF<T>`` metadata pointers is only the ``value_schema`` and
  ``delta_value_schema``: both alias the canonical
  ``TimeSeriesReference`` atomic, the C++ value type that backs a
  reference token at runtime. Direct nested references are normalised:
  asking the registry for ``REF<REF<T>>`` returns the existing
  ``REF<T>`` schema. Nested ``REF`` is therefore not a distinct schema
  shape and must not create a separate output alternative.
- ``TSL`` keys its delta on ``int`` because the slot id (an integer
  index) is the universal path identifier into a slot store. The
  registry uses the standard ``int`` scalar (``Int`` / ``std::int64_t``) when it
  synthesises a TSL delta schema.
- ``TSD`` and ``TSS`` deltas are bundles because the per-tick change
  set carries more than one category. ``TSD`` collapses *added* and
  *updated* into a single ``modified`` map: any key present in
  ``modified`` carries the new or replacement value for that key, and
  ``removed`` carries the keys that were dropped. This matches the
  Python TSD delta surface and avoids the need to disambiguate added-
  vs-updated at the schema level. ``TSS`` keeps the ``added`` /
  ``removed`` split because membership-only sets have no notion of
  *update*.
- The standard ``TSD`` delta value schema remains the compact
  ``Bundle{removed, modified}`` shape above. The eventual TSD delta
  view should still expose convenience queries for ``added``,
  ``removed``, ``updated``, and ``modified``. ``added`` and
  ``updated`` are view-level classifications derived from the current
  slot state and the collapsed ``modified`` map; they are not separate
  fields in the stored delta value.
- ``TSW`` (tick) has a fixed-size list as ``value_schema`` because the
  rolling window length is known up front; duration-based windows fall
  back to a dynamic list since the count of elements per window varies
  with tick rate. In TSData, that list schema is exposed by custom
  window-backed list ops: tick windows read from fixed cyclic storage
  and duration windows read from timestamped queue storage. The delta
  remains the scalar element added at the current evaluation time.

Reference Behaviour
-------------------

``REF<T>`` has two separate roles in the runtime:

- as a value, it is a ``TS<TimeSeriesReference>`` whose current value
  is a reference token;
- as an output alternative, it can participate in representation
  changes when an output is bound to an input whose reference markers
  differ from the output's canonical schema.

The value token is intentionally opaque to user code. A
``TimeSeriesReference`` can be empty, can name a single output through
an internal output handle, or can hold a non-peered collection of child
references. Public code may pass the token around and may request a
reference token from an input view, but it must not dereference the
token to obtain an output handle or output view. Internal binding and
output-alternative machinery may access the handle through a restricted
interface.

This is a deliberate design boundary. The runtime currently supports
dereferencing only inside the time-series alternative mapping logic,
where a ``REF<T>`` output is exposed as ``T`` for a bound input. Public
``REF`` output dereference should not be added unless a concrete use
case appears that cannot be represented cleanly through normal graph
wiring and output alternatives.

Reference conversion is output-owned. ``RefLink`` is only created by
output alternative infrastructure, not by user code and not by the
plain input binding path. Taking a reference token from an input is
still valid; it produces a ``TimeSeriesReference`` value that can later
be applied to another input or stored in a ``REF`` output.

Alternative representations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When an input is bound to an output and their schemas differ only by
reference markers, the binding process asks the output to expose an
alternative representation. The output view being bound is the
conversion anchor. The runtime does not automatically promote a child
request to a root-wide conversion.

For example, given an output:

.. code-block:: text

   TSL[TS[int], Size[2]]

binding only the first element to an input of type:

.. code-block:: text

   REF[TS[int]]

creates a local alternative for the first element:

.. code-block:: text

   TS[int] exposed as REF[TS[int]]

It does not create a whole-list alternative such as:

.. code-block:: text

   TSL[REF[TS[int]], Size[2]]

The whole-list alternative is created only when the list output itself
is bound to an input of that whole-list reference shape.

The to-``REF`` strategy treats reference markers as the only shape
difference handled by this mechanism. Identical schemas use the native
output representation. Otherwise, validation is based on the fully
dereferenced shapes:

.. code-block:: text

   dereference(schema_requested) == dereference(schema_owned)

``schema_owned`` is the canonical schema of the output view being
bound, and ``schema_requested`` is the schema required by the input
view. ``dereference`` recursively removes ``REF`` markers by replacing
``REF<T>`` with the dereferenced shape of ``T``. Direct
``REF<REF<T>>`` schemas are normalised by the registry and are not a
separate alternative form.

The current to-``REF`` storage shape is:

.. code-block:: text

   0..n structural prefixes (TSB, fixed TSL, TSD)
       followed by a REF[remaining requested shape] leaf

At the ``REF`` leaf, materialisation stops. The leaf value is the
``TimeSeriesReference`` taken from the peered source output view at
that position. Any structure below that reference belongs to the
referenced output if it is later dereferenced.

Each requested path has exactly one materialised conversion leaf: it
is either the top-level alternative itself or a child below a
``TSB``, fixed-size ``TSL``, or ``TSD`` prefix. Once that leaf is
created, the current output does not build additional representation
state below it.

For keyed alternatives, the canonical key store remains the source of
truth. Exposing a canonical ``TSD[K, V]`` as ``TSD[K, REF[V]]`` uses
the reusable ``TSDProxy`` TSData component. ``TSDProxy`` takes a source
``TSD`` view, adapts the source ``TSS`` key-set surface, and owns only
the alternative value slots. It is not an ad-hoc associative child map:
child values are stored in slot storage aligned to the source key
slots, so the requested value shape can be recreated without
duplicating keys.

There are two important keyed to-``REF`` shapes:

.. code-block:: text

   direct value conversion:
       source:    TSD[K, V]
       requested: TSD[K, REF[V]]

   keyed path conversion:
       source:    TSD[K, TSB/TSL/TSD[..., V]]
       requested: TSD[K, TSB/TSL/TSD[..., REF[V]]]

In the direct value conversion, the proxy's child storage type is the
``REF[V]`` leaf, so each mirrored source slot materialises a
``TimeSeriesReference`` directly. In the keyed path conversion, the
proxy's child storage type is the normal requested structural child. The
proxy still exists because the key slots are dynamic and must remain
aligned with the source, but the child is populated by walking the
requested structure until the first ``REF`` leaf is reached. Any
nested ``TSD`` encountered on that path owns its own proxy.

``TSDProxy`` does not support external mutation. Its values are
materialised by a value-builder function supplied by the caller. The
builder receives the proxy, source slot id, target child TSData view,
source child TSData view, and evaluation time. The to-``REF`` alternative
uses that hook either to construct the appropriate
``TimeSeriesReference`` leaf or to construct the requested structural
child that contains such a leaf, but the proxy itself is not
reference-specific.

The proxy registers with the source key-set ``SlotObserver`` protocol
for structural slot alignment and with normal TSData modification
notification to learn the evaluation time. Source key insert/remove/erase
events construct, retain, and destroy proxy value slots. When source
modification notification arrives, the proxy reads the source
added/removed slot surfaces and materialises the affected values.
Ordinary source child value ticks do not rebuild an already
materialised to-``REF`` child because the output identity did not
change.

Alternative storage
~~~~~~~~~~~~~~~~~~~

Each root output may own a nullable alternative store. The store is
allocated on the first alternative request. It is a cache and
ownership boundary; it does not change the semantic rule that
conversion is anchored at the output view being bound.

The primary cache key is:

.. code-block:: text

   [starting output view, requested schema]

The starting output view is the concrete output view handed to input
binding: the root output identity plus the ``TSData`` binding/data
pair at that navigation point. The requested schema is the input-side
schema the target link wants to observe. Since canonical schema matches
return the original output view directly, every entry in this cache is
by definition an alternative representation.

The requested schema drives the alternative storage and ops. For
example:

- if the starting output view is not ``REF`` and the requested schema
  is ``REF``, the alternative is a ``REF`` view of that starting
  view;
- if the starting output view is ``REF`` and the requested schema is
  not ``REF``, the alternative is an unpacked view through a
  ``RefLink``;
- once a canonical ``REF`` is reached, this output stops tracking
  deeper structure. Anything below the reference belongs to the
  referenced output.

The actual alternative state is instantiated only where a conversion
is required. For the non-``REF`` to ``REF`` case, that state is a
``ToRefAlternativeState``. It owns a ``TSData`` allocation for the
requested schema and the returned ``TSOutputHandle`` points at this
alternative ``TSData`` while retaining the root output identity.
For the ``REF`` to non-``REF`` case, the state is a
``RefLinkAlternativeState``. It subscribes to the source ``REF`` TSData
level, owns endpoint-plan TSData for the requested exposed schema, and
applies each ``TimeSeriesReference`` tick into TargetLink leaves.

This means the alternative view is TSData-backed and exposes the
requested shape through normal view APIs. A request for
``REF[TS[int]]`` stores a real ``TimeSeriesReference`` value. A
request for ``TSL[REF[TS[int]], Size[2]]`` stores a fixed-list TSData
allocation whose two children are real ``REF`` leaves. A request for
``TSD[int, REF[TS[int]]]`` stores a ``TSDProxy`` whose keys are read
from the source dictionary and whose value-builder creates the
requested ``REF`` values directly. A request for
``TSD[int, TSB[{items: TSL[REF[TS[int]], Size[2]]}]]`` also stores a
``TSDProxy``, but the proxy child is the requested ``TSB`` structure;
the two ``REF`` leaves are materialised inside that child.

The inverse (from-``REF``) direction supports the same interior
positions as to-``REF``. The keyed shape mirrors the keyed to-``REF``
conversion:

.. code-block:: text

   direct value conversion:
       source:    TSD[K, REF[V]]
       requested: TSD[K, V]

   keyed path conversion:
       source:    TSD[K, TSB/TSL[..., REF[V]]]
       requested: TSD[K, TSB/TSL[..., V]]

Both store a ``TSDProxy`` over the source dictionary whose child
binding is the requested element's *input endpoint* binding (TargetLink
leaves). The proxy's value-builder applies each source element - a
``REF`` leaf binds the link from the reference; a subtree that is
already the requested shape binds one whole-subtree link directly to
the source child; a structural subtree recurses; a nested ``TSD``
element with interior references owns its own proxy. Because element
references can retarget, a from-``REF`` proxy binds with the
``OnChildTick`` child-refresh policy: a live slot whose source child
ticks re-runs the builder (to-``REF`` proxies keep the default
``StructureOnly`` policy - a child value tick does not change the
materialised identity). The proxy only marks itself modified when the
refreshed child actually recorded at that time, so same-reference
re-publication stays silent. A key inserted and written within one
source mutation notifies observers at insert time; child reads
lazily re-run the builder for a slot whose source child recorded at
or after its last build and whose value never materialised. Fixed ``TSB``/``TSL``
sources with interior references expose the requested shape through a
normal non-peered endpoint whose ref-positions are applied from the
corresponding source children; a ``TSD`` below a fixed prefix is only
supported when that subtree is already reference-free (deeper
combinations remain unimplemented and throw).

The inverse request ``REF[TS[int]]`` exposed as ``TS[int]`` stores a
single TargetLink-backed endpoint. When the source reference ticks, the
link binds, rebinds, or unbinds that endpoint. A request such as
``REF[TSB[{bid: TS[float], ask: TS[float]}]]`` exposed as the bundle
shape stores a non-peered bundle endpoint with TargetLink leaves. A
peered reference to a whole bundle is split into child links; a
non-peered reference binds each child link from the corresponding child
reference.

The alternative has its own time-series tracking. Binding or rebinding
marks the alternative modified at the bind time. Ordinary ticks of the
underlying non-``REF`` output do not tick the alternative ``REF`` value,
because the reference identity did not change. Dynamic collection
membership is different: when a source ``TSD`` reports added or removed
keys, the proxy updates its aligned value slots and marks the
alternative modified for that evaluation time. Source child value ticks
do not change an already materialised to-``REF`` child.

Only ``TSD`` introduces live proxy behaviour. Static ``TSB`` and
fixed-size ``TSL`` prefixes are created once and need no ongoing
interaction with their source after their children have been
materialised, unless a descendant path crosses a ``TSD``. In that case
the descendant ``TSD`` owns the subscription and synchronisation; the
static prefix remains a structural container.

A starting output view can have multiple cached alternatives when
different inputs request different schemas. Likewise, the root output
view and a nested child output view are different starting views even
when they request the same schema. For example:

.. code-block:: text

   TSB[{ts: TSL[TS[int], Size[2]]}]

If an input binds the whole output as:

.. code-block:: text

   REF[TSB[{ts: TSL[TS[int], Size[2]]}]]

the root element gets a local ``REF`` alternative. If another input
binds only ``ts[0]`` as:

.. code-block:: text

   REF[TS[int]]

the cache keys differ because the starting output views differ. The
root alternative does not replace the child alternative and the child
alternative does not become a second root.

REF boundaries
~~~~~~~~~~~~~~

A ``REF`` position is a navigation boundary for alternatives. The
current output owns alternative state only up to the first ``REF`` on
each requested path. Anything below that boundary belongs to the
output referenced by the ``TimeSeriesReference``.

Compatibility validation compares the full dereferenced shape of the
requested schema with the full dereferenced shape of the owned schema.
Materialisation does not follow the same depth. This lets the runtime
reject invalid bindings early without forcing it to build alternative
slots for data that will only become reachable after a reference is
dereferenced.

For example, consider an output whose canonical schema is:

.. code-block:: text

   TSL[REF[TSL[REF[TS[int]], Size[2]]], Size[3]]

and an input that wants:

.. code-block:: text

   TSL[REF[TSL[TS[int], Size[2]]], Size[3]]

The validation rule succeeds because both schemas dereference to:

.. code-block:: text

   TSL[TSL[TS[int], Size[2]], Size[3]]

The materialisation rule is narrower. The outer ``TSL`` shapes match,
but at each element the current output reaches a ``REF`` boundary.
The current output validates that the referenced schema:

.. code-block:: text

   TSL[REF[TS[int]], Size[2]]

has the same dereferenced shape as the requested referenced schema:

.. code-block:: text

   TSL[TS[int], Size[2]]

However, the current output only materialises the outer list slots and
the reference-boundary alternative. It does not build slots for the
inner fixed list. If the reference is later dereferenced, the
referenced output is asked for the ``TSL[TS[int], Size[2]]``
alternative, and that output owns any further alternatives required
below that point.

Nested references therefore form a chain of one-hop output
alternatives. Each ``RefLink`` resolves one reference value, subscribes
to one current target, and delegates any deeper representation request
to that target output. Validation remains bounded by the finite schema
shape being requested: once the requested shape reaches a leaf, or a
``REF`` boundary whose wrapped target shape has been validated, the
current validation step is complete.

Input binding negotiation
~~~~~~~~~~~~~~~~~~~~~~~~~

Inputs do not bind their non-peered prefixes. A ``TSInput`` plan holds
non-peered structure only as navigation and modification state; binding
happens at peered target-link positions. Each target link knows the
schema it expects to observe.

Binding starts with a canonical output view or handle. The caller does
not pre-select an alternative representation. Instead, the target link
asks the output for binding data matching the target link's expected
schema:

.. code-block:: text

   input target link expected schema + canonical output view
       -> output binding data for the expected schema

If the expected schema is the same as the output view's canonical
schema, the output returns canonical binding data. If the schemas differ
only by reference markers, the output validates the request with:

.. code-block:: text

   dereference(schema_requested) == dereference(schema_owned)

and returns binding data for an alternative representation. That
binding data points either at the original output storage or at the
root-owned alternative store described above. It also carries the
alternative ops needed to navigate, read, and subscribe to the exposed
shape.

For example, binding an input target link expecting:

.. code-block:: text

   REF[TS[int]]

to a canonical output view of:

.. code-block:: text

   TS[int]

asks the output to expose ``TS[int]`` as ``REF[TS[int]]`` and binds the
target link to the returned alternative binding data. Conversely,
binding an input expecting ``TS[int]`` to an output whose canonical
schema is ``REF[TS[int]]`` asks the output for an internal dereferenced
alternative and binds to that returned shape. This is the alternative
store crossing the reference boundary; it is not a public operation for
node code to dereference a ``REF`` output directly.

This negotiation rule applies to all target-link binding, not just
``REF``. A bind operation on an input receives a canonical output view;
the input-side target link states what schema it wants; the output-side
binding provider returns the canonical or alternative output shape that
satisfies that request.

Recursion is automatic: the metadata for a nested ``TSD<Str,
TSL<TS<Float>>>`` reads its inner ``TSL``'s ``delta_value_schema``
directly off the inner schema, so the registry never has to recompose
known schemas.
