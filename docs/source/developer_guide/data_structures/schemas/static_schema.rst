Static Schema
=============

Static schema is the C++-friendly way to describe a schema. The
"dynamic" path described in *Scalar Schemas* and *Time-Series Schemas*
goes through ``TypeRegistry`` factory calls (``ts(int_meta)``,
``tsd(string_meta, ts_int_meta)``, …) and returns interned metadata
pointers at runtime. Static schema lets a C++ developer express the
same shapes as **types** at compile time:

.. code-block:: cpp

   using PriceTick = TSB<"PriceTick",
                         Field<"bid",   TS<Float>>,
                         Field<"ask",   TS<Float>>,
                         Field<"size",  TS<Int>>>;

   using QuoteFeed = TSD<Str, PriceTick>;

The compiler carries the shape; a small set of *descriptor* traits
turn that compile-time type into the matching runtime metadata
pointer (``TSValueTypeMetaData *``) by calling into ``TypeRegistry``.
Descriptors deliberately do **not** maintain their own static caches:
all canonicalisation and pointer stability comes from the registry's
intern tables. A node implementation written against these markers
gets both type safety at the call site and one canonical interned
schema at runtime.

This page describes the compile-time vocabulary, the descriptor
bridge to the runtime registry, and the design choices that follow
from the C++-first goal. The selector layer that pairs markers with
runtime views (``In<>``, ``Out<>``, ``State<>``) is described under
*Selector wrappers* below; for the scalar time-series path it is now
implemented, and node authoring built on it is described in *Wiring*.

Why a compile-time vocabulary
-----------------------------

The dynamic registry path is the source of truth at runtime, but it
has two costs at the call site:

1. **Untyped pointers.** The factory returns ``const
   TSValueTypeMetaData *``. The compiler cannot tell whether two
   pointers describe the same shape; two callers must agree by
   convention.
2. **Order-of-construction sensitivity.** The factory must be
   reachable, the metadata must already be registered, and the
   composition must be assembled correctly each time.

Static schema folds both away. A type alias like ``QuoteFeed``
*is* the schema. The compiler enforces shape compatibility on
function signatures and returns, and the descriptor traits ensure
the runtime metadata pointer is canonical and shared with every
other use of the same alias.

Compile-time vocabulary
-----------------------

Every static-schema type is a small, mostly-empty struct template
whose template parameters carry the shape. They never instantiate at
runtime — they exist purely to drive the descriptor traits.

``fixed_string<N>``
    Compile-time string literal usable as a non-type template
    parameter. Used to embed names (field names, bundle names, type-
    variable names) directly into a schema type.

``TS<T>``
    Scalar time-series of one atomic ``T``. Mirrors the runtime ``TS``
    kind.

``TSS<T>``
    Time-series set of ``T``.

``TSD<K, V>``
    Time-series dict. ``K`` is a scalar (``Int``, ``Str``,
    …); ``V`` is itself a static-schema time-series type.

``TSL<T, N = 0>``
    Time-series list of ``T`` (a static-schema time-series type).
    ``N == 0`` is the dynamic form; ``N > 0`` is fixed-size.

``TSW<T, period, min_period = 0>``
    Tick-based sliding window. (Duration-based windows are not yet
    expressible as a compile-time type; use the runtime registry for
    that case.)

``REF<TSchema>``
    Reference to a target time-series schema.

``SIGNAL``
    The signal kind. Has no parameters.

``Field<Name, TSchema>``
    Named field used inside ``Bundle`` / ``UnNamedBundle`` /
    ``TSB`` / ``UnNamedTSB``. ``Name`` is a ``fixed_string``; the
    schema is either a value-layer type (for ``Bundle``) or a
    time-series schema (for ``TSB``).

``Bundle<Name, Fields...>`` / ``UnNamedBundle<Fields...>``
    Value-layer compound. ``Bundle`` has a name (nominal identity);
    ``UnNamedBundle`` is structural. Maps to the runtime
    ``bundle(name, fields)`` and ``un_named_bundle(fields)``
    factories respectively, with the same nominal-vs-structural
    identity rules described in *Scalar Schemas > Bundle*.

``Owned<T>``
    Value-layer owner indirection. Its runtime schema is
    ``TypeRegistry::owned(T)`` and its inline storage is exactly one pointer;
    the pointee is allocated on first mutable access. Runtime
    ``recursive_bundle`` is the preferred declaration path for a
    self-recursive nominal Bundle because a directly recursive C++ marker
    alias cannot be completed in one template declaration.

``TSB<Name, Fields...>`` / ``UnNamedTSB<Fields...>``
    Time-series compound. Same named/un-named split as ``Bundle``.

``ScalarVar<Name, Constraints...>`` / ``TsVar<Name, Constraints...>``
    Type variables. Used when a node implementation is generic over a
    scalar or time-series type. Resolution happens at wiring time;
    the descriptor traits report ``is_concrete() == false`` for
    schemas that contain unresolved variables.

Bridge to the runtime registry
------------------------------

The descriptor traits are the single bridge between the compile-time
vocabulary above and the runtime ``TypeRegistry``. They are the only
place a static-schema type touches the registry.

``scalar_descriptor<T>``
    For a scalar type ``T``, exposes:

    - ``is_concrete()`` — ``true`` for concrete scalar types, ``false``
      for ``ScalarVar``.
    - ``value_meta()`` — returns the canonical
      ``ValueTypeMetaData *`` for the scalar by calling
      ``TypeRegistry::register_scalar<T>(...)``.

``schema_descriptor<TSchema>``
    For a time-series schema type, exposes:

    - ``is_concrete()`` — recurses through component schemas.
    - ``ts_meta()`` — returns the canonical ``TSValueTypeMetaData *``
      for ``TSchema`` by resolving the shape through ``TypeRegistry``.

``field_descriptor<Field<Name, TSchema>>``
    Exposes the field's static name and forwards
    ``ts_meta()`` (for TSB fields) / ``value_meta()`` (for Bundle
    fields) to the appropriate inner descriptor.

The descriptor specialisations cover every marker type. Adding a new
schema marker is a matter of adding a corresponding descriptor
specialisation; the rest of the registry path is unchanged.

Example (rendered as the doc lands alongside an actual implementation):

.. code-block:: cpp

   using PriceTick = TSB<"PriceTick",
                         Field<"bid",  TS<Float>>,
                         Field<"ask",  TS<Float>>>;

   const TSValueTypeMetaData *meta =
       schema_descriptor<PriceTick>::ts_meta();
   // meta is the same pointer as
   //   registry.tsb("PriceTick", {{"bid", registry.ts(...)}, ...})

The registry intern tables are the only cache. Repeated descriptor
calls may re-enter ``TypeRegistry``, but equivalent shapes resolve to
the same canonical metadata pointer. This keeps static descriptors
correct across test-only registry resets and avoids a second lifetime
model outside the registry.

Generic schemas
---------------

A schema that contains a ``TsVar`` or ``ScalarVar`` is *generic*. The
descriptor's ``is_concrete()`` returns ``false`` and ``ts_meta()`` /
``value_meta()`` returns ``nullptr`` (the concreteness is AND-folded
through children, so ``TSD<ScalarVar, TsVar>`` is generic too). Wiring
resolves each variable to a concrete type before constructing a runtime
node, at which point a *resolved* schema (with the variable replaced by
the concrete type) becomes concrete and looks up its registry pointer
normally.

This is **implemented** (``include/hgraph/types/type_resolution.h``). A
``ResolutionMap`` binds variable names to concrete metadata; two families
mirror the descriptor recursion:

- ``ts_resolver<S>`` / ``scalar_resolver<T>`` — **substitute** the bound
  metadata into the (otherwise compile-time) schema, producing the
  resolved registry pointer (a concrete leaf ignores the map, so the same
  resolver serves concrete and generic schemas uniformly);
- ``ts_unifier<S>`` / ``scalar_unifier<T>`` — **bind** variables by
  matching a pattern schema against a concrete runtime meta (e.g. a
  connected input port's schema), recursing through containers.

A node is authored once over its variables; ``StaticNodeSignature``
exposes ``is_generic()`` and resolution-map overloads of its schema
accessors, and ``NodeBuilder::implementation<T>(const ResolutionMap&)``
builds the resolved node — see *Schemas > Node Schemas > Generic
Resolution* and *Graph Wiring*. This is the C++ counterpart of (and the
seed for the eventual bridge to) Python type-variable resolution.

Selector wrappers
-----------------

The selector layer pairs a static-schema marker with a runtime view, so a
node ``eval`` can take a typed handle as a parameter:

.. code-block:: cpp

   struct SumNode
   {
       static constexpr auto name = "sum";

       static void eval(In<"lhs", TS<Int>> lhs,
                        In<"rhs", TS<Int>> rhs,
                        Out<TS<Int>>      out)
       {
           out.set(lhs.value() + rhs.value());
       }
   };

**Selectors derive from the type-erased view** (defined in
``<hgraph/types/static_node.h>``). A selector is a *zero-overhead, compile-time
typed facade* that **inherits** the matching erased view for its kind and layers
typed sugar on top — it does **not** re-implement or duplicate the view's data
ops:

.. code-block:: text

   In<Name, TS<T>>     : TSInputView      Out<TS<T>>     : TSOutputView
   In<Name, TSS<T>>    : TSSInputView     Out<TSS<T>>    : TSSOutputView
   In<Name, TSL<C,N>>  : TSLInputView     Out<TSL<C,N>>  : TSLOutputView
   …                   : TS{B,D,W}…View   …              : TS{B,D,W}…View

The erased views are themselves CRTP (``TSLInputView : TSInputTypedView<…> :
TSTypedTimeSeriesView<…, TSInputView>``); a selector inherits the whole erased
surface (``modified()`` / ``valid()`` / ``value()`` / ``delta_value()`` /
``size()`` / ``at(i)`` / ``modified_items()`` / ``as_set()`` / …) for free and
adds only:

- ``using schema = …`` and ``static constexpr field_name`` — the compile-time
  signature, consumed by ``StaticNodeSignature`` / ``input_selector_traits`` /
  ``schema_descriptor<S>`` for plan and metadata construction;
- a constructor from the endpoint view (so ``arg_provider`` builds it from a
  ``TSInputView`` / ``TSOutputView``);
- **typed sugar**: ``In<TS<T>>::value() -> T`` (shadows the erased
  ``value() -> ValueView``; the raw form stays reachable through the base);
  collection helpers such as ``In<TSS<T>>::added()/removed()/contains()``,
  ``In<TSD<K,V>>::valid_items()``, ``In<TSB>::field<"...">()``, and
  ``In<TSW<T>>::operator[]``; output helpers such as ``Out<TS<T>>::set/apply``,
  ``Out<TSS<T>>::add/remove/clear``, ``Out<TSD<K,V>>::operator[]``,
  ``Out<TSB>::field<"...">()``, and ``Out<TSW<T>>::push``.

A selector carries **no state beyond the inherited view** (``Name`` is a
``static constexpr``; the output's ``evaluation_time`` comes from the view), so
the typed layer is free at runtime and everything routes through the canonical
type-erased ops — which the *Allocation, Plans and Ops* performance contract
requires (a typed path is only justified when it is faster than the erased ops).

``SIGNAL`` is special on the input side. ``In<Name, SIGNAL>`` is a tick
subscription and can bind to any time-series output schema; all other input
selectors require the normal schema-equivalent output.

**Recursive, nestable children.** Because a container selector *is* its erased
view, child access falls straight out of the inherited ``at(i)`` and composes to
**any** child schema, to any depth:

.. code-block:: cpp

   // TSL of sets:  out[i] is an Out<TSS<Int>>, in[i] is an In<"", TSS<Int>>
   static void eval(In<"l", TSL<TSS<Int>, 2>> l, Out<TSL<TSS<Int>, 2>> out)
   {
       for (auto &&[idx, child] : l.modified_items())  // inherited from TSLInputView
           out[idx].add(*l[idx].added().begin());      // out[idx] : Out<TSS<Int>>
   }

   // TSL of TSL of TS:  in[i][j].value()  (recursion bottoms out at a scalar)
   In<"g", TSL<TSL<TS<Int>, 2>, 3>> g;  g[0][1].value();

``In<Name, TSL<C,N>>::operator[](i)`` returns ``In<"", C>`` (a name-agnostic
child facade — ``fixed_string`` admits ``""``); ``Out<TSL<C,N>>::operator[](i)``
returns ``Out<C>``. The same recursion applies through ``TSD`` key access and
``TSB`` compile-time field access.

This selector recursion (and the delta construction below) is purely a
compile-time/value-layer concern and works for any child. The TSData runtime
now covers fixed ``TSL`` children across the implemented non-``REF`` kinds:
``TS``, ``SIGNAL``, ``TSS``, ``TSD``, fixed and dynamic ``TSL``, ``TSB``,
and ``TSW``. Slot, dynamic-list, and window children are owned as child
auxiliary storage, while fixed children recurse through the parent
value/auxiliary layout. Dynamic ``TSL`` storage is grow-only until the
``TSL`` delta schema grows a structural removal surface.

**Deltas are canonical type-erased Values.** A selector does *not* introduce a
parallel delta representation. The delta of any time-series is the canonical
``Value`` whose schema is the runtime ``delta_value_schema`` (``TS<T>`` /
``SIGNAL`` / tick-count ``TSW<T,...>`` → scalar; ``TSS<T>`` →
``Bundle{added: Set<T>, removed: Set<T>}``; ``TSD<K,V>`` →
``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``; ``TSL<C,N>`` →
``Map<int, delta(C)>``; ``TSB{f...}`` →
``Bundle{f: delta(f_schema)...}``; recursive). ``In<…>::delta()`` is just the inherited
``delta_value()``. To **construct** a collection delta for tests/wiring, recursive
builder functions produce the canonical ``Value`` (see *Allocation, Plans and Ops
> Value builders* and *Testing Graphs in C++*):

.. code-block:: cpp

   set_delta<Int>({Int{1}, Int{2}}, {})                       // -> Bundle{added:{1,2}, removed:{}}
   list_delta<TSS<Int>>({{0, set_delta<Int>({Int{1}}, {})}})  // -> Map<int, Bundle>
   dict_delta<Str, TS<Int>>({{Str{"a"}, Int{1}}}, {Str{"b"}})   // -> Bundle{removed, modified}
   tsb_delta<PriceTick>(101.0, std::nullopt, 5)     // ask is typed-null

Comparison and display go through the value-layer ops (``Value::equals`` —
order-independent for sets/maps — and ``to_string``); no wrapper type
re-implements them.

``State<T>`` is a typed handle into node-local (value-layer) state, with
``get()`` / ``set(v)``. ``Scalar<"name", T>`` is a named wiring-time scalar.
How these markers drive node construction — ``StaticNodeSignature`` and
``NodeBuilder::implementation<T>()`` — is described in *Wiring*.

**Generic selectors.** ``In<Name, TsVar<"S">>`` / ``Out<TsVar<"S">>`` /
``Scalar<Name, ScalarVar<"T">>`` are implemented variable forms: ``In`` /
``Out`` are the bare erased view (no typed sugar — there is no concrete element
type), and ``Scalar`` holds its configured value type-erased as an owned
``Value``. A node authored over them is *generic*; the variables resolve at
wiring time (see *Generic schemas* above and *Graph Wiring*). The framework's
own utility nodes (``replay`` / ``record`` / ``const_`` / ``debug_print`` /
``null_sink``) are authored this way — **one** implementation each, the schema
flowing as data, driven by the runtime ``capture_delta`` / ``apply_delta``
rather than per-type code for the replayable kinds. ``REF`` is a separate
binding surface.

Planned, landing with their runtime layers:

- ``PythonScalar<"name", Type<"my.module.type">>`` — a named Python-object
  scalar whose expected Python type is named (as a string) for type-checking;
  omitting the type (``PythonScalar<"name">``) defaults to ``object`` (any
  Python object / generic);
- automatic recordable-state record/replay attachment / ``recordable_id`` wiring;
- named state (``State<TSchema, Name>``).

Status
------

Today: ``fixed_string``, marker types (``TS``, ``TSS``, ``TSD``,
``TSL``, ``TSW`` tick-only, ``REF``, ``SIGNAL``, ``Field``, named &
un-named ``Bundle`` / ``TSB``, ``ScalarVar``, ``TsVar``), the
``scalar_descriptor`` / ``schema_descriptor`` / ``field_descriptor``
traits, and the **derive-from-view node-authoring selectors** —
``In<Name, TS<T>>`` / ``REF<T>`` / ``SIGNAL`` / ``TSS<T>`` / ``TSD<K,V>`` /
``TSL<C,N>`` / ``TSB`` / tick-count ``TSW<T,...>`` (and the ``Out<…>`` duals)
deriving from their erased input/output views, plus ``RecordableState<TSchema>``,
``State<T>`` and
``Scalar<Name, T>``, plus transparent injectables such as ``GlobalStateView``,
``EvaluationClockView`` and ``NodeScheduler`` — together with ``StaticNodeSignature`` and
``NodeBuilder::implementation<T>()`` (see *Wiring*). ``State<T>`` and
``RecordableState<TSchema>`` are mutually exclusive for a static node:
recordable state replaces local state when the state must be exposed through a
hidden time-series output. That hidden output is for system-level recording and
replay only: it is not a normal output port and must not activate or schedule the
owning node. C++ system wiring can deliberately extract it with
``recordable_state(port)``; this creates a special edge source root rather than a
normal output path. ``TSL`` is
**recursive**:
its child may be any supported non-``REF`` time-series schema, nested
arbitrarily, and replayable canonical delta ``Value`` shapes are built/compared
through the value layer. These build on the live
``TypeRegistry`` API (including the named/un-named bundle split), so static
schemas register and resolve identically to direct factory calls, and the
implemented non-``REF`` time-series paths wire from a node struct through to a
running graph.

**Generic-resolution substitution is now implemented** (node-level): the
``TsVar`` / ``ScalarVar`` selectors, ``ResolutionMap`` + ``ts_resolver`` /
``ts_unifier`` (``type_resolution.h``), ``StaticNodeSignature::is_generic()`` +
the resolution-map schema overloads, and the wiring-time resolution in ``wire<>``
(unify from input ports, infer from scalar values, or supply explicitly via
``ts_type<>()`` / an explicit output schema). See *Graph Wiring*.

Deferred until the relevant runtime layer lands: automatic recordable-state
record/replay attachment / ``recordable_id`` wiring, specialized push-source
node/builder support, named state, duration-based ``TSW``, the Python-export
bridge, and **graph-level** generic resolution (aggregating node-level
resolution across a sub-graph).
(``NodeScheduler`` / ``SingleShotScheduler`` injection is now implemented; see
*Authoring Nodes in C++*.)
