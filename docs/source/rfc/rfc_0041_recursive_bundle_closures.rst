RFC 0041: Recursive Bundle Closures
===================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-19
:Target: ``TypeRegistry``, the static schema, and generated language modules

Summary
-------

Add one registry operation that declares a recursive Bundle together with
everything its recursive edges reach, and one static-schema marker that names
such an edge. ``TypeRegistry::recursive_bundle_closure`` describes the
reachable specializations on demand, groups them into strongly connected
components, and registers each component as one ``recursive_bundles`` batch.
``Edge<Target>`` is a ``NominalBundle`` field that owns a value of another
generated struct, which may be the enclosing one or one declared later.

The operation is the single implementation of recursive batching. The static
schema uses it for compiled C++, and the HGL compiler's direct-wiring backend
uses it for the same programs, so both register identical schemas.

Motivation
----------

``recursive_bundle`` and ``recursive_bundles`` register a recursive batch whose
members the caller already knows. A program does not state its batches. The
HGL language admits recursive struct fields (HGL ADR 0012): a struct reaches
itself, another struct of its module, a generic application of itself such as
``Pair<Y, X>`` inside ``Pair<X, Y>``, or an abstract parent. Which
specializations form one batch is known only once the arguments are, and two
specializations can coincide (``Pair<i64, i64>``).

The static schema could not express any of this. ``Owned<T>`` needs ``T``'s
schema, a ``NominalBundle`` alias cannot name itself or a struct declared
after it, and its descriptor registers through ``bundle()``, which would
recurse without end through a recursive field.

The HGL direct-wiring backend already computes the batches with Tarjan's
algorithm over specializations. Generated C++ needs the same answer. Two
implementations of one registration rule would drift, so the rule moves into
the registry and both callers use it.

Registry operation
------------------

.. code-block:: cpp

   struct RecursiveBundleRequest
   {
       RecursiveBundleDefinition definition{};
       std::vector<std::pair<std::size_t, std::string>> edges{};
   };

   using RecursiveBundleDescriber =
       std::function<RecursiveBundleRequest(std::string_view qualified_name)>;

   const ValueTypeMetaData *TypeRegistry::recursive_bundle_closure(
       std::string_view root, const RecursiveBundleDescriber &describe);

A request is one specialization. Its ``definition`` is an ordinary
``RecursiveBundleDefinition`` whose recursive edge fields carry neither a
``type`` nor an ``owned_target``; ``edges`` gives each such field's position
and the qualified name of the specialization it owns. The describer is called
at most once per unregistered name reachable from ``root`` and must return a
definition whose qualified name is the one requested.

The registry walks the requests with Tarjan's algorithm and registers each
strongly connected component when it closes, after every component it reaches:

* a component of one specialization with no edge to itself is an ordinary
  named Bundle, registered through ``bundle()`` with each edge typed
  ``owned(target)``, so a later ``bundle()`` call with the same description is
  the same schema;
* any other component is one ``recursive_bundles`` batch: an edge inside the
  component becomes a batch index, an edge leaving it owns the registered
  target;
* a name that is already registered is reused and not described.

The operation returns the schema of ``root``. It is linear in the number of
reachable specializations and their fields. It does not hold the registry lock
while the describer runs, so a describer may realize other schemas, including
other closures. A race between two threads registering the same closure is
resolved by re-reading each name before its batch registers.

Static schema
-------------

.. code-block:: cpp

   template <typename TTarget> struct Edge {};

``TTarget`` is a class with a nested ``value_type`` that is a
``NominalBundle``. It may be incomplete where the ``Edge`` is written:

.. code-block:: cpp

   struct Forest;

   struct Tree
   {
       using value_type = NominalBundle<"forest", "Tree", false, BundleParents<>, BundleArguments<>,
                                        Field<"value", Int>, Field<"forest", Edge<Forest>>>;
   };

   struct Forest
   {
       using value_type = NominalBundle<"forest", "Forest", false, BundleParents<>, BundleArguments<>,
                                        Field<"tree", Edge<Tree>>>;
   };

* ``scalar_descriptor<Edge<T>>`` is ``owned(T::value_type)``; an edge is
  concrete when ``T``'s generic arguments are.
* A ``NominalBundle`` with an ``Edge`` field registers through
  ``recursive_bundle_closure``. Its describer describes every generated struct
  its edges reach, so ``Tree`` and ``Forest`` above are one batch whichever is
  used first.
* ``NominalBundle`` exposes its ``parents`` and ``arguments`` types.
* In a ``NominalTSB`` an edge is ``TS<Edge<T>>``: one endpoint whose value is
  the owner the Bundle stores, so the TSB's value schema is the Bundle itself.
  The owner is storage, not type (``value_schema_without_storage``), so the
  endpoint binds where ``TS<T::value_type>`` is expected.

Compatibility and ABI
---------------------

The operation and the marker are additions. ``Owned<T>``, ``recursive_bundle``
and ``recursive_bundles`` are unchanged. No ops table or storage layout
changes. Existing ``NominalBundle`` declarations without an ``Edge`` register
exactly as before.

Implementation status
---------------------

Implemented with this proposal: the registry operation, ``Edge``, the
``NominalBundle`` descriptor path, and core tests for self, mutual, generic,
coinciding and outside edges and for the temporal shape. The HGL compiler
adopts the operation in its direct-wiring backend and ``Edge`` in its C++
emitter as part of HGL ADR 0012.

Acceptance criteria
-------------------

* A self-recursive, a mutually recursive and a generic ``NominalBundle``
  declared with ``Edge`` register as batches whose owners name their members,
  whichever member is realized first.
* An edge to a struct outside its component, such as an abstract parent, owns
  that struct's schema, and the struct stays an ordinary named Bundle.
* Coinciding specializations register once.
* The HGL direct-wiring backend registers through the operation, and a
  program's schemas are the same from direct wiring and from generated C++.

References
----------

* :doc:`../developer_guide/data_structures/schemas/scalar`, "Recursive Bundle
  fields".
* :doc:`../developer_guide/data_structures/schemas/static_schema`.
* HGL ADR 0012, ``language/docs/design/decisions/0012-recursive-struct-fields.md``.
