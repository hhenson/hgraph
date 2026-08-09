Type Reflection: Non-goal and Migration
=======================================

Decision
--------

C++-first hgraph **does not provide the Python-first** ``Hg*TypeMetaData`` **type-reflection
family** — ``HgTypeMetaData`` and every subclass (``HgScalarTypeMetaData``,
``HgTimeSeriesTypeMetaData``, ``HgTSTypeMetaData``, ``HgTSDTypeMetaData``,
``HgTSLTypeMetaData``, ``HgTSSTypeMetaData``, ``HgTSBTypeMetaData``,
``HgREFTypeMetaData``, ``HgTSWTypeMetaData``, ``HgCONTEXTTypeMetaData``,
``HgTypeOfTypeMetaData``, the ``*OutTypeMetaData`` variants, and the scalar
subclasses). The associated constructors and engine helpers —
``HgTypeMetaData.parse_type`` / ``parse_value``, ``resolve``,
``build_resolution_dict``, ``generic_rank``, ``type_vars``, ``is_resolved``,
``cpp_type`` / ``cpp_native`` / ``CppNative[T]`` — are likewise not provided.

**Rationale.** In upstream hgraph the runtime is Python-first, so the
type-metadata objects double as the wiring engine's internal representation
*and* as a user-visible reflection API. In C++-first hgraph the C++ layer **is** the
runtime; type identity, resolution, overload ranking, and storage selection all
live in C++ (``types/metadata/``, ``type_registry``, ``value_plan_factory``).
Re-exposing a parallel Python metadata-object hierarchy would be exactly the
"parallel abstraction" the v2 design forbids — a second way to describe types
that must be kept in lock-step with the C++ one.

**Consequence for compatibility.** Upstream tests that import or assert against
the ``Hg*TypeMetaData`` classes are **not compatibility targets**; their
failures/collection-blocks are ignored (see ``roadmap.rst`` Accepted
Deviations). User code that depended on the reflection API **must be migrated**;
this page is the migration guide. Migration is cheap because the load-bearing
needs are already met and the rest reduces to a small convenience layer.

What real code actually needed
------------------------------

A review of every upstream tier file that touches the reflection family (the 11
files under ``_types/``, ``_wiring/test_lift.py``, ``_wiring/test_map.py``,
``nodes/test_frame.py``) found that **~85% of the touches test the reflection /
resolution machinery itself** — the parser's own unit tests
(``test_type_meta_data``), the typevar-inference engine
(``test_type_meta_resolve``, ``test_schema_base``), the overload-ranking engine
(``test_operator_rank``), and upstream's own Python→C++ bridge
(``test_ts_cpp_type``, ``test_cpp_type_integration``, ``test_cpp_native_compound_scalar``).
None of that is something an application or operator author ever writes.

The genuine user needs collapse to five patterns, only two of which are
load-bearing:

======  =======================================================  ============
Need    Pattern                                                  Status
======  =======================================================  ============
P1      Introspect a resolved node/graph signature               **works today**
P3      Compare / match a type against a plain python type        **works today**
P5      See through ``REF[...]``                                  partial today
P2      Structurally decompose a TS type (key/value/element/…)    convenience layer
P4      Query a schema's fields (bundle / compound-scalar)        convenience layer
======  =======================================================  ============

The alternative — available today
---------------------------------

**Types are first-class comparable values.** ``TS[int]``, ``TSD[str, TS[int]]``
etc. are ordinary hashable, equality-comparable Python objects. There is no
"parse" step and no metadata object to build:

.. code-block:: python

   # upstream
   HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]])
   # C++-first hgraph — the annotation IS the reflection value
   TSD[str, REF[TS[int]]]

   assert TS[int] == TS[int]           # True
   assert TS[int] != TS[float]         # True
   {TS[int], TSD[str, TS[int]]}        # usable in sets / dict keys

**P1 — signature introspection works directly.** A wired node/graph exposes its
resolved signature; the type-bearing fields are plain comparable types:

.. code-block:: python

   sig = my_node.signature
   sig.input_types            # {'a': TS[int], 'b': TS[str]}   (name -> type)
   sig.output_type            # TS[str]
   assert sig.output_type == TS[str]
   assert sig.input_types["a"] == TS[int]

This replaces the dominant upstream pattern
``sig.output_type == HgTypeMetaData.parse_type(TS[str])`` with a direct
comparison — a mechanical ``parse_type(X) -> X`` rewrite.

**P3 — compare / match** is the same equality; no ``.matches`` call needed for
the common case (``port.output_type == TS[int]``).

**P5 — reference predicate** is on every type expression:
``REF[TS[int]].is_ref`` is ``True``.

The alternative — structural decomposition (convenience layer)
--------------------------------------------------------------

For P2/P4 — decomposing a TS type into its parts, or listing a schema's fields —
C++-first hgraph adds a small ``hgraph.reflection`` helper module. It is a **thin Python
wrapper over primitives the C++ bridge already exposes** (``_hgraph.TsType``
predicates ``is_ts``/``is_tsd``/``is_tsl``/``is_tss``/``is_tsb``/``is_ref``/
``fixed_size``, and ``_hgraph.tsd_element_ts`` / ``tsl_element`` /
``ts_field_types`` / ``ts_value_vt``) — **no new C++ is required**. Every
accessor returns a plain, comparable python/TS type:

.. code-block:: python

   from hgraph.reflection import scalar_type, key_type, value_type, \
       element_type, size, fields, resolved_type, dereference, is_bundle, is_reference

   scalar_type(TS[int])            # int
   key_type(TSD[str, TS[int]])     # str
   value_type(TSD[str, TS[int]])   # TS[int]
   element_type(TSL[TS[int], 3])   # TS[int]
   size(TSL[TS[int], 3])           # 3
   fields(MyBundle)                # {'a': TS[int], 'b': TS[str]}  (ordered)
   resolved_type(m[OUT])           # public TS expression for a resolver binding
   dereference(REF[TS[int]])       # TS[int]
   is_reference(REF[TS[int]])      # True
   is_bundle(MyBundle)             # True

This is the smallest surface that unblocks the genuine P2/P4 needs. It is a
deliberate, closed set — it does **not** reintroduce ``parse_type`` /
``resolve`` / ``isinstance(HgTSDTypeMetaData)``.

.. note::

   **Status: available.** The load-bearing migration (P1/P3/P5) works against
   the shipped surface directly; ``hgraph.reflection`` adds the P2/P4
   convenience layer (``python/hgraph/reflection.py``, tests in
   ``python/tests/test_reflection.py``). It is a Python-only wrapper over
   existing bridge primitives — no C++ was added.

Migration reference
-------------------

=====================================================  ==================================================
Python-first 0.5 (``Hg*TypeMetaData``)                 C++-first hgraph
=====================================================  ==================================================
``HgTypeMetaData.parse_type(X)``                       ``X``
``m == HgTypeMetaData.parse_type(TS[int])``            ``t == TS[int]``
``m.matches(TS[int])``                                 ``t == TS[int]`` (equality)
``sig.output_type`` (an ``Hg*TypeMetaData``)           ``sig.output_type`` (a comparable TS type)
``sig.input_types[name]``                              ``sig.input_types[name]`` (a comparable TS type)
``m.value_scalar_tp`` (TS/TSS payload)                 ``reflection.scalar_type(t)``
``m.key_tp`` / ``m.value_tp`` (TSD)                    ``reflection.key_type(t)`` / ``value_type(t)``
``m.element_type`` / ``m.size`` (TSL)                  ``reflection.element_type(t)`` / ``size(t)``
``m.meta_data_schema`` (TSB / compound scalar)         ``reflection.fields(t)``
``m.py_type`` (resolved TS binding)                    ``reflection.resolved_type(t)``
``m.dereference()`` / ``m.has_references``             ``reflection.dereference(t)`` / ``is_reference(t)``
``isinstance(m, HgTSDTypeMetaData)``                   ``t.handle.is_tsd`` / ``reflection.is_tsd(t)``
``m.parse_value(v)`` / ``m.resolve(rd)``               no equivalent (C++ engine concern)
``build_resolution_dict`` / ``generic_rank``           no equivalent (C++ engine concern)
=====================================================  ==================================================

Out of scope (intentionally dropped)
------------------------------------

- ``parse_type`` / ``parse_value`` as public reflection constructors returning
  metadata objects. Compare against plain python/TS types instead.
- ``resolve`` / ``build_resolution_dict`` / ``generic_rank`` / ``type_vars`` /
  ``is_resolved`` — the typevar-resolution and overload-ranking engine. This is
  internal to C++ wiring; there is no user-facing equivalent.
- ``cpp_type`` / ``cpp_native`` / ``CppNative[T]`` — upstream's Python→C++
  storage bridge. C++-first hgraph is always native, so the concept is moot.
- Direct construction of, and ``isinstance`` checks against, the
  ``Hg*TypeMetaData`` classes. Use the equality/predicate/decomposition forms
  above.

Design note — REF wrapping
--------------------------

Some resolved signatures wrap values in ``REF[...]`` as a wiring-internal
artifact (e.g. ``map_`` passes multiplexed values by reference, so an upstream
``sig.output_type`` reads ``TSD[str, REF[TS[int]]]`` rather than
``TSD[str, TS[int]]``). The convenience decomposition accessors expose the type
**as resolved** (REF-wrapped where the engine wraps it); callers that want the
logical shape apply ``reflection.dereference`` explicitly. This keeps the
accessors faithful to the runtime rather than silently rewriting it. *(If a
dereferenced-by-default variant proves more convenient in practice, it is added
as a separate explicit accessor, not by changing this default.)*
