RFC 0036: Reference Transparency Has Four Owners
=================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-06
:Target: ``include/hgraph/types/metadata/type_registry.h``,
         ``include/hgraph/types/time_series/endpoint_schema.h``,
         ``include/hgraph/types/graph_wiring.h`` (``NamedPort``),
         ``include/hgraph/types/operator_type_resolution.h``,
         ``include/hgraph/types/time_series/ts_output/base_view.h``,
         ``include/hgraph/types/time_series/ts_input/base_view.h``,
         ``include/hgraph/runtime/nested_bindings.h``,
         ``src/hgraph/runtime/{graph,race_tsd_node,shared_output_node}.cpp``,
         ``include/hgraph/lib/std/operators/impl/{collection,container,control,higher_order,tsb_itemwise}_impl.h``,
         ``python/py_type_system.cpp``, ``python/py_wiring.cpp``,
         ``python/hgraph/_wiring/{_compose,_core,_graph,_runner}.py``
:Related: developer guide ``operators.rst`` ("REF transparency"),
          ``writing_nodes.rst`` ("A generic time-series parameter binds the
          DEREFERENCED type"), ``nested_graphs.rst`` (boundary REF modes),
          ``testing.rst`` ("Architecture ratchets"); PRs #654 (the REF
          matcher), #655 (boundary modes); the 2026-09-04 fix-series
          retrospective (invariant 1)

Summary
-------

``REF[X]`` is type-compatible with ``X``; a generic time-series parameter binds
the *dereferenced* type; the reference is a routing detail a consumer never
sees unless it declares ``REF``. That rule is documented, and since #654 the
type-pattern matcher and the binding step apply it. Yet it is still applied by
hand in three layers, and the architecture ratchets pin the copies:

.. list-table::
   :header-rows: 1
   :widths: 32 10 58

   * - Ratchet
     - Count
     - What the copies do
   * - ``stdlib-ref-dereference``
     - 64 (+3)
     - ``TypeRegistry::dereference(...)`` inside std operator impls; the
       ratchet scans ``include/hgraph/lib/std/operators/impl`` only, and
       three more copies sit in ``src/hgraph/lib/std/operators``
       (``data_frame_impl.cpp`` ×2, ``convert_target.cpp``)
   * - ``wiring-ref-handling``
     - 22
     - ``is_ref`` / ``dereferenced`` / ``ref_target`` in Python wiring
   * - ``runtime-ref-kind-probes``
     - 7
     - ``TSTypeKind::REF`` probes in the runtime

The 64 std-operator sites were expected to fall away once the matcher bound
the dereferenced type ("one line for ~86 deletions", retrospective). They did
not, because they are not one thing. Read one by one they are five intents,
and only one of them is redundant with the matcher:

.. list-table::
   :header-rows: 1
   :widths: 46 8 46

   * - Intent
     - Sites
     - Example
   * - dereference the operator's *own* bound argument before reasoning
       about it
     - 27
     - ``registry.dereference(context.args[0].port.schema)``
       (``control_impl.h``), ``dereference(ts[0].schema)``,
       ``dereference(lhs.schema)`` (``tsb_itemwise_impl.h``)
   * - the *element* of a collection whose elements may be references
     - 9
     - ``registry.dereference(tsd->element_ts())`` (``container_impl.h``,
       ``collection_impl.h``)
   * - compare a nested graph's or branch's output schema with a declared
       one *through* references
     - 17
     - ``time_series_schema_equivalent(registry.dereference(combiner_graph.output_schema),
       registry.dereference(element))`` (``higher_order_impl.h``)
   * - the value type of a key source
     - 7
     - ``TypeRegistry::instance().dereference(key.schema)``
       (``higher_order_impl.h``)
   * - wrap a possibly-referenced element as a canonical reference
     - 4
     - ``registry.ref(registry.dereference(element->element_ts()))``
       (``control_impl.h``)

Each intent has a natural owner in the type layer, and for three of them the
owner already exists. This RFC names the four owners, gives each a public
spelling, and retires every hand-written copy:

1. **A declared port observes its declared shape.** The argument helpers of
   ``operator_type_resolution.h`` (``time_series_schema_at`` and friends)
   already dereference by default; a ``NamedPort<Name, Schema>`` gains
   ``observed()`` -- the port with its schema dereferenced unless ``Schema``
   declares ``REF``. Raw ``context.args[i].port.schema`` and raw ``ts.schema``
   reads become reads of ``SchemaRefMode::Direct`` for ``REF``-declared
   parameters only.
2. **Elements are observed by value.** ``TypeRegistry::value_element_ts``
   answers the element of a ``TSD`` / ``TSL`` with references followed. It
   is the schema-level statement of the binding rule's exception ("a value
   collection whose elements are references is left as supplied, because its
   element links dereference on access").
3. **Equivalence is reference-transparent.** ``time_series_value_equivalent``
   sits beside ``time_series_schema_equivalent`` and compares after following
   references on both sides. ``input_accepts_output_schema`` already does
   exactly this; it becomes the first caller.
4. **A structural hop through a reference resolves the reference first.**
   ``TSOutputView::through_reference()`` (resolve the from-REF alternative
   when the view's schema is ``REF``) replaces the four identical probes in
   ``graph.cpp`` and ``nested_bindings.h``. The one *per-tick* probe, the
   shared-output capture's stability test, is decided when the target
   **binds**: the target link records whether its bound output is a
   reference at bind and rebind (events), and
   ``TSInputView::bound_target_is_reference()`` reads that record.

Canonical reference wrapping needs no new owner: ``TypeRegistry::ref`` is
already idempotent on a reference, so ``ref(dereference(x))`` *is* ``ref(x)``.
The Python wiring copies are the same intents spelled in Python; the bridge
exposes the owners once (``value_port``, ``value_element_ts``) and the DSL stops
spelling ``is_ref`` and ``dereferenced``.

The type layer holds copies of the equivalence rule too, and they migrate
with the owner: ``schema_equivalent_after_dereference`` in
``ts_output/alternative.cpp`` (two callers), the paired comparison in
``static_node.h`` (``Out<REF>`` target checks) and ``input_accepts_output_schema``
all become calls to ``time_series_value_equivalent``; a new ratchet,
``paired-dereference-comparisons``, holds the ``dereference(a) … dereference(b)``
comparison shape at one occurrence (the owner) across the whole tree.

Outcome: ``stdlib-ref-dereference`` 67 → 0 with its roots widened to
``include/hgraph/lib/std`` and ``src/hgraph/lib/std``,
``runtime-ref-kind-probes`` 7 → 0, ``wiring-ref-handling`` 22 → 0,
``paired-dereference-comparisons`` introduced at its count and taken to 1, and
one more rule site (``value-consumer-source-callers`` 3 → 4, the port's
``observed()``), all recorded here.

Motivation
----------

The retrospective's first invariant was REF transparency, and its diagnosis
was that the rule lived in consumers: when the matcher stripped only the
outer reference, every consumer that noticed dereferenced for itself, and
every consumer that did not produced *plausible nonsense* (``log_`` printed a
token). #654 moved the matcher's part to the owner. The consumer copies stayed,
and they stayed for a reason that the retrospective's "~86 deletions" missed:
most of them are not the matcher's rule at all.

An operator that builds its output schema by hand -- ``reduce`` comparing its
combiner's output with the collection element, ``switch_`` comparing a
branch's output with the declared one, ``keys_`` reading a key source,
``getitem_`` on a ``TSD`` whose elements are ``map_`` references -- has to
reason about schemas that binding never rewrites: nested graph outputs (the
result schema is *never rewritten*, ``operators.rst``), collection elements
(only the top-level reference is rewritten, ``writing_nodes.rst``), and the
raw ports handed to ``compose`` and ``resolve_default_types`` before any
binding has happened. Each such site re-derives "what will I actually observe
here" with a bare ``dereference``, which is correct today and is exactly the
shape of code the retrospective identified as a rule applied at the wrong
layer: when the rule changes (the structural adaptation of #649 was such a
change), sixty-four places have to agree.

The same reasoning produced the Python copies. ``_runner.py`` reconstructs
"a REF graph output records its dereferenced values, a TSB with REF fields
records a structural bundle of per-field projections" -- which is the
binding rule's structural descent, rewritten in Python -- and ``_core.py``
dereferences by hand to find the fields of a ``REF[TSB]``.

Naming the owners is what makes the count fall and stay down: the ratchets
then measure "did someone re-derive the rule" rather than "did someone
dereference".

Terms
-----

*Observed schema*
    The schema a consumer sees through a bound input: the referenced value
    for a non-``REF`` declaration, the reference itself for a ``REF``
    declaration. The binding step (``adapt_source_for_input``,
    ``value_argument``) produces it; this RFC makes it readable before binding.

*Value element*
    The element schema of a ``TSD`` / ``TSL`` with every reference followed:
    what an access through the element link observes.

*Reference-transparent equivalence*
    Two time-series schemas are value-equivalent when they are equivalent
    after following references on both sides. ``REF[TS[int]]`` and
    ``TS[int]`` are value-equivalent; ``REF[TS[int]]`` and ``TS[float]`` are
    not.

Ownership boundary
------------------

* ``TypeRegistry`` owns the schema-level rules: ``dereference`` (existing),
  ``ref`` (existing, idempotent), ``value_element_ts`` (new).
* ``endpoint_schema.h`` owns schema equivalence: ``time_series_schema_equivalent``
  (existing) and ``time_series_value_equivalent`` (new).
* The port and argument descriptors own the observed schema:
  ``operator_type_resolution.h``'s helpers (existing, ``Dereference`` by
  default) and ``NamedPort::observed()`` (new).
* The views own the structural hop: ``TSOutputView::through_reference()``
  and ``TSInputView::bound_target_is_reference()`` (new).
* The bridge exposes the owners to the DSL: ``value_port`` (new),
  ``value_element_ts`` (new); ``ref_target`` and ``is_ref`` remain for
  authors who ask for a reference explicitly.
* Std operators, the runtime and the Python wiring call the owners and hold no
  copy of the rule. Structure-preserving packing (``tsb_itemwise``, the
  ``map_`` / ``switch_`` / ``mesh_`` machinery) still routes references
  deliberately; it uses ``SchemaRefMode::Direct`` and the raw port where it
  means the reference, and the owners where it compares or observes values.

C++ contract
------------

Type registry
~~~~~~~~~~~~~

.. code-block:: cpp

   class TypeRegistry
   {
       /** The element of a TSD / TSL as an access through the element link
           observes it: the element schema with every reference followed.
           Throws for a schema with no element. */
       [[nodiscard]] const TSValueTypeMetaData *value_element_ts(const TSValueTypeMetaData *collection);

       /** Existing; documented as idempotent: ref(REF[X]) is REF[X], so a
           consumer never writes ref(dereference(x)). */
       [[nodiscard]] const TSValueTypeMetaData *ref(const TSValueTypeMetaData *referenced_ts);
   };

Equivalence
~~~~~~~~~~~

.. code-block:: cpp

   /** time_series_schema_equivalent after following references on both
       sides (the REF transparency rule of operators.rst applied to two
       schemas). */
   [[nodiscard]] HGRAPH_EXPORT bool time_series_value_equivalent(const TSValueTypeMetaData *a,
                                                                 const TSValueTypeMetaData *b);

``input_accepts_output_schema`` calls it instead of dereferencing both sides
itself.

Ports and arguments
~~~~~~~~~~~~~~~~~~~

.. code-block:: cpp

   template <StringLiteral Name, typename S>
   struct NamedPort : Port<S>
   {
       /** The port as this parameter observes it: dereferenced unless ``S``
           declares a REF (value_consumer_source applied by the declaration). */
       [[nodiscard]] WiringPortRef observed() const;
   };

``time_series_schema_at(context, i)`` keeps its ``Dereference`` default. The
rule for a std operator becomes: read an argument schema through the helper
(observed) or through ``SchemaRefMode::Direct`` (the reference, for a
``REF``-declared parameter); never through ``context.args[i].port.schema``
followed by a ``dereference``.

Views
~~~~~

.. code-block:: cpp

   class TSOutputView
   {
       /** The view of the referenced output when this view's schema is a
           REF (its from-REF alternative), otherwise this view. A structural
           hop (key set, field, element) goes through it. */
       [[nodiscard]] TSOutputView through_reference() const;
   };

   class TSInputView
   {
       /** True when the bound output is a reference (its target can move).
           Read from the target link's record, which is written when the
           link binds or rebinds -- never a schema probe on the tick path. */
       [[nodiscard]] bool bound_target_is_reference() const noexcept;
   };

The target link (``TSInputTargetLinkStorage``) records the flag beside the
target handle in its bind and rebind paths; the capture node's ``eval`` reads
it, and the node's REF handling mode is thereby decided when the target
binds, as ``nested_graphs.rst`` requires of every node.

Bridge contract
---------------

``_hgraph.value_port(port)`` returns the port as a value consumer observes it:
``value_consumer_source`` at the top level and the structural descent of
``adapt_source_for_input`` below it (a ``TSB`` / fixed ``TSL`` of references
becomes a structural port of per-field / per-element value projections). This
is the one thing ``_runner.py`` and ``_core.py`` reconstruct today.
``_hgraph.value_element_ts(ts_type)`` mirrors the registry's helper. ``is_ref``
and ``ref_target`` stay for explicit reference handling (``race``, the
services' reference contracts).

Python contract
---------------

No user-facing change. ``WiringPort.dereferenced`` remains as the explicit
"give me the referenced port" the DSL exposes to authors; the wiring
*machinery* (``_graph.py``'s graph-output rule, ``_core.py``'s ``as_dict`` /
``as_scalar_ts`` / field-name lookup, ``_compose.py``'s reduce identity,
``_runner.py``'s record port and producer annotation) goes through
``value_port`` / ``value_element_ts`` and spells neither ``is_ref`` nor
``dereferenced``.

Runtime representation and operator/dispatch semantics
------------------------------------------------------

Nothing changes at runtime. Every owner is a build-time (wiring or
node-construction) operation; the one per-tick site,
``shared_output_node.cpp``'s stability probe, reads a flag the target link
recorded at bind time instead of probing the target's schema kind. The flag
is one ``bool`` beside the link's target handle; no delta, storage or
ops-table layout changes and no ABI bump.

Compatibility, migration and serialisation
------------------------------------------

* **Operators**: behaviour-preserving by construction where the copy was
  the owner's rule; where a site turns out to differ from the owner (a
  consumer that dereferenced something the rule says it should not have, or
  did not dereference something it should), the difference is a finding and
  is recorded in the implementation status, not silently normalised. The
  REF consumer sweep (``test_ref_consumer_sweep.py``: 8 shapes × 9 REF
  sources × ~60 consumers) and the ported operator suites are the behaviour
  pins.
* **Extensions**: ``dereference`` and ``ref`` stay public; an extension that
  dereferences its own arguments keeps working. The new helpers are
  additive.
* **Serialisation**: none affected.

Performance and memory
----------------------

None. All owners are wiring-time; ``value_element_ts`` and
``time_series_value_equivalent`` are the same registry lookups the copies made.
``through_reference()`` is the same ``binding_for(...).view(...)`` the probes
made, at edge-binding time.

Installed-extension and ABI consequences
----------------------------------------

Additive C++ API in the type registry, the endpoint-schema header, the port
descriptor and the two view classes; no ops-table or record ABI change. The
bridge gains two functions.

Alternatives considered
-----------------------

* **Delete the 64 sites as redundant with the matcher.** Rejected: only the
  27 own-argument sites are the matcher's rule; the rest reason about schemas
  binding never rewrites (nested graph outputs, elements, raw ``compose``
  ports). Deleting them would reintroduce the *plausible nonsense* failures.
* **Rewrite nested graph output schemas at compile time** so the comparisons
  need no dereference. Rejected: ``operators.rst`` fixes that a producer's
  computed ``REF`` output is never rewritten (a ``default`` that yields a
  ``REF`` must stay a ``REF`` for its consumers); the comparison is the
  right place for transparency, and it wants one owner.
* **Describe collection elements by value at binding** (extend the top-level
  rewrite into elements). Rejected in ``writing_nodes.rst`` already: it
  would install a from-REF link per element for consumers that only hold
  tokens; an interleaved A/B showed no runtime difference either way.
* **One ``dereference_for_declaration(schema, declared)`` helper** used by
  all 64 sites. Rejected: it keeps the sites deciding when to call it; the
  owners here make the decision at the descriptor (``observed()``), the
  registry (``value_element_ts``) and the comparison
  (``time_series_value_equivalent``), so a new site cannot forget.

Unresolved questions
--------------------

* Whether ``NamedPort::observed()`` should be what ``Port<S>`` *is* for
  non-``REF`` declarations (implicit) or an explicit call. This RFC proposes
  explicit, because ``compose`` functions of the structure-preserving
  operators need the raw port and an implicit rewrite would silently change
  what they route. If the sweep shows every ``compose`` wants ``observed()``,
  the implicit form is a follow-up amendment.
* ``_runner.py``'s record-port descent for a fixed ``TSL`` of references
  builds a structural ``TSL`` of dereferenced element projections; whether
  ``value_port`` should do the same for a *dynamic* ``TSL`` (RFC 0031) or
  keep the port as supplied is settled during implementation by the parity
  pins, and recorded.

Acceptance criteria and test plan
---------------------------------

1. ``stdlib-ref-dereference`` 0 over ``include/hgraph/lib/std`` **and**
   ``src/hgraph/lib/std``, ``runtime-ref-kind-probes`` 0,
   ``wiring-ref-handling`` 0, ``paired-dereference-comparisons`` 1 (the
   owner), ``value-consumer-source-callers`` 4, each lowered (or raised,
   with this record) in the same change; ``schema_equivalent_after_dereference``
   no longer exists.
2. C++: ``TypeRegistry::value_element_ts`` on ``TSD[K, REF[TS[int]]]``,
   ``TSD[K, TS[int]]``, ``TSL[REF[TS[int]], 2]`` and a dynamic ``TSL``;
   ``time_series_value_equivalent`` over the reference-transparency cases of
   ``operators.rst``; ``through_reference()`` on a REF and a non-REF output.
3. Python: ``test_ref_consumer_sweep.py`` unchanged; the ported operator and
   wiring suites unchanged; ``test_registry_snapshot.py`` unchanged.
4. Platforms: macOS local gate, Linux GCC 14 core-only and Python builds,
   Windows MSVC, before each merge.

Implementation plan
-------------------

Four PRs, each green on the full gate, each lowering its ratchet:

1. **Owners**: ``value_element_ts``, ``time_series_value_equivalent`` with
   every copy of the rule migrated (``input_accepts_output_schema``,
   ``schema_equivalent_after_dereference`` and its two callers, the
   ``static_node.h`` target check) and the ``paired-dereference-comparisons``
   ratchet introduced, ``NamedPort::observed()``, the two view accessors
   and the target link's bind-time record, the bridge functions;
   ``operators.rst`` and ``writing_nodes.rst`` name the owners; the ``ref``
   idempotence is documented.
2. **Std operators** (67 → 0, roots widened to the source directory): the
   five intents mapped to their owners, one impl header or unit per commit;
   differences found are recorded.
3. **Runtime** (7 → 0): the four structural hops, the two ``race_tsd``
   wrappings (``ref`` idempotence), the stability probe through the
   bind-time record.
4. **Python wiring** (22 → 0): ``value_port`` / ``value_element_ts`` in
   place of the hand-written descent; the parity pins settle the dynamic
   ``TSL`` question.

Implementation status
---------------------

* **PR 1 (owners)** -- landed: ``TypeRegistry::value_element_ts``;
  ``time_series_value_equivalent`` with every type-layer and runtime copy of
  the rule migrated (``input_accepts_output_schema`` and the nominal-upcast
  check of ``adapt_source_for_input``, the output-direction check of
  ``ts_pattern_match``, the dispatch upcast check, the alternative binding
  check of ``ts_output.cpp``, ``schema_equivalent_after_dereference`` and
  its two callers, the ``static_node.h`` and ``shared_output_node.cpp``
  target checks, the forwarding-tree check of ``nested_bindings.h``) and
  the ``paired-dereference-comparisons`` ratchet introduced at 12 (the
  owner plus eleven std operator copies for PR 2); ``NamedPort::observed()``
  (``value-consumer-source-callers`` 3 → 4); ``TSOutputView::through_reference()``;
  ``TSInputView::bound_target_is_reference()`` reading the record
  ``TSInputTargetLinkState::target_is_reference`` that ``bind_impl`` writes
  on every bind and rebind and ``detach_target`` / ``source_invalidated``
  clear; the bridge's ``value_port`` and ``value_element_ts``; ``ref``
  idempotence documented on the registry.

  *Finding:* the shared-output capture's stability test has two halves, and
  both are properties of the handle the link binds: the output's schema is a
  ``REF``, *or* the output is itself reached through a target link (the
  from-REF alternative a value input binds to a ``REF`` output through, a
  chained adaptor's relay). The record covers both, so
  ``bound_target_is_reference()`` reads "the bound output can move", and PR
  3 replaces the probe with the accessor alone.
* PR 2 (std operators), PR 3 (runtime), PR 4 (Python wiring): pending.

References
----------

* Developer guide: ``operators.rst`` ("REF transparency"),
  ``writing_nodes.rst`` ("A generic time-series parameter binds the
  DEREFERENCED type"), ``nested_graphs.rst``, ``testing.rst``
  ("Architecture ratchets").
* PR #654 (the REF matcher: a type variable binds the dereferenced type),
  PR #655 (``SwitchOutputMode``, boundary REF modes), #649 (structural
  adaptation of a REF source).
* The 2026-09-04 fix-series retrospective, invariant 1.
