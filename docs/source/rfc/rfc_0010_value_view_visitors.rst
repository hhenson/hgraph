RFC 0010: Type-Erased Value Visitors
====================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-30
:Target: Public C++ value views

Summary
-------

Add callable-based visitation for a live ``ValueView``. The visitor dispatches
the erased value's semantic shape to the existing specialised view classes,
plus a new ``AtomicView`` tag for the open-ended scalar case. Populated
``Any`` boxes are transparent: the visitor unwraps them until it reaches the
contained concrete shape. ``AnyView`` remains the explicit API for managing
the box and is not a visitor alternative.

The implementation uses the existing schema discriminator, operation tables,
and borrowed view projections. It adds no virtual hierarchy, heap allocation,
closed list of C++ scalar types, or value-operation slot.

Motivation
----------

Algorithms that genuinely vary by live value shape currently repeat a switch
on ``ValueTypeKind`` and construct ``TupleView``, ``MapView``, and the other
specialised cursors themselves. This is error-prone around open-ended atomic
types, borrowed lifetimes, mutation capability, polymorphic bindings, and the
``Any`` box.

The runtime already carries all information needed to perform that dispatch.
Centralising it gives independently built extensions one checked public
contract while leaving operations intrinsic to the erased value in
``ValueOps``.

Ownership boundary
------------------

Value visitation belongs in the public C++ SDK because independently built
extensions consume ``ValueView`` and may register their own atomic scalar
types. C++ remains the source of truth for value schemas, storage, operations,
and views.

Python receives no new visitor API. Python already performs dynamic dispatch
over converted values. Existing C++ nodes used through the Python bridge retain
the same visible behaviour, and production adopters require matching C++ and
Python compatibility coverage.

Public C++ contract
-------------------

The installed header ``<hgraph/types/value/visitor.h>`` provides:

.. code-block:: cpp

   template<class... Handlers>
   decltype(auto) visit(const ValueView &value, Handlers&&... handlers);

Handlers form one overload set. After transparent ``Any`` unwrapping, dispatch
selects:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Declared kind
     - Handler argument
   * - ``Atomic``
     - ``AtomicView``
   * - ``Tuple``
     - ``TupleView``
   * - ``Bundle``
     - ``BundleView``
   * - ``List``
     - ``ListView``
   * - ``Set``
     - ``SetView``
   * - ``Map``
     - ``MapView``
   * - ``CyclicBuffer``
     - ``CyclicBufferView``
   * - ``Queue``
     - ``QueueView``

``AtomicView`` is a move-only, two-word tag over ``ValueView``. It does not
enumerate scalar C++ types. A handler tests or extracts a scalar it understands
with the existing ``holds_alternative<T>()``, ``try_as<T>()``, and
``checked_as<T>()`` operations:

.. code-block:: cpp

   return visit(
       value,
       [](AtomicView scalar) {
           if (scalar.holds_alternative<ExtensionScalar>()) {
               return handle(scalar.checked_as<ExtensionScalar>());
           }
           return fallback_scalar(scalar);
       },
       [](MapView map) { return handle_map(map); },
       [](ValueView other) { return handle_other(other); });

A shape-specific handler takes precedence over the ``ValueView`` catch-all.
Every concrete alternative must be accepted directly or by that catch-all.
Every selected branch returns ``void`` or the same exact safe value type.

Reference results and lazy ``Range`` / ``KeyValueRange`` results are rejected.
The selected wrapper is a temporary borrowed cursor, and a lazy range can
retain storage or projection context associated with it. Callers consume the
range in the handler or return an owned materialisation.

Transparent ``Any`` semantics
-----------------------------

``AnyValue`` is the schema and storage concept for an owning dynamic box.
``AnyView`` exists to inspect or mutate that box:

.. code-block:: cpp

   AnyView box = value.as_any();
   box.has_value();
   box.get();
   box.begin_mutation().set(replacement);
   box.begin_mutation().clear();

It is not a semantic shape of the value stored inside the box and is therefore
not a visitor alternative. ``visit`` repeatedly unwraps populated ``Any``
layers and dispatches the contained value:

.. code-block:: cpp

   Value boxed{any_type()};
   boxed.as_any().begin_mutation().set(Value{ExtensionScalar{42}});

   visit(boxed.view(), [](AtomicView scalar) {
       // Receives ExtensionScalar, not AnyView.
   }, [](ValueView) {});

An empty ``Any`` contains no visitable value and raises
``std::invalid_argument``. Code whose policy assigns meaning to an empty box
checks it explicitly through ``AnyView`` before visiting. This keeps absence
out of the semantic shape set.

Unwrapping is iterative rather than recursive. Nested boxes therefore do not
consume call stack, and every populated layer is transparent. The contained
view retains read or write capability derived from its owner. Opening a
mutation remains explicit; visiting writable storage does not itself enter a
mutation phase.

Declared shape and polymorphic values
-------------------------------------

Dispatch uses the live view's declared ``ValueTypeKind``. It does not call
``ValueView::concrete()``:

* registered and enum scalar schemas dispatch as ``AtomicView``;
* fixed and shaped arrays dispatch as ``ListView``;
* structural, named, and owned recursive records dispatch as ``BundleView``;
* polymorphic concrete projection remains an explicit caller policy.

This matches the specialised view contract. A caller that intentionally wants
the active concrete representation first calls ``concrete()`` and visits the
result.

Visitor and type-erasure boundary
---------------------------------

The visitor is for caller-owned algorithms whose behaviour varies by live
value shape. It does not replace behaviour already owned by the erased value's
operations:

* hashing, equality, comparison, formatting, and Python conversion remain in
  ``ValueOps``;
* assignment, copying, ownership projection, and concrete projection remain in
  ``ValueOps`` and ``StoragePlan``;
* wiring-time schema, overload, codec-plan, and factory selection continue to
  inspect metadata directly;
* recursive traversal, key selection, empty-box policy, and polymorphic
  projection remain explicit algorithm policies.

This prevents visitation from becoming a second type-erasure mechanism or a
per-tick replacement for decisions that belong at wiring time.

Representation and performance
------------------------------

The implementation is header-only. It performs a value-kind switch and
constructs the selected borrowed view through an internal trusted path after
the kind has already been checked. Specialised views still resolve and verify
the operation-table subclass required for their public methods, but they do
not repeat semantic-kind validation. Public direct constructors remain
checked.

The endpoint and value visitors share only generic callable composition,
result selection, and borrowed-result safety traits. Their projection and
dispatch logic remain independent.

The change adds no data member, virtual table, type-record field,
operation-table slot, registry entry, or runtime dependency.
``sizeof(ValueView)`` and ``sizeof(AtomicView)`` remain two machine words.

A focused benchmark compares a complete caller-written kind switch with
``visit`` for an atomic value and a structured value. Each result is the
median of 21 samples of 200,000 dispatches:

.. list-table::
   :header-rows: 1
   :widths: 23 17 17 17 14 12

   * - Platform
     - Value shape
     - Manual ns/op
     - Visitor ns/op
     - Visitor change
     - Allocations
   * - macOS arm64, AppleClang 21
     - Atomic
     - 3.378
     - 2.002
     - 40.7% faster
     - 0
   * - macOS arm64, AppleClang 21
     - Bundle
     - 5.522
     - 3.725
     - 32.5% faster
     - 0
   * - Linux x86_64, GCC 15.2
     - Atomic
     - 3.467
     - 1.728
     - 50.2% faster
     - 0
   * - Linux x86_64, GCC 15.2
     - Bundle
     - 6.544
     - 3.585
     - 45.2% faster
     - 0

The visitor's trusted post-switch projection avoids repeating the public
constructor's semantic-kind validation. Both paths are allocation-free.

Compatibility
-------------

The API is additive. Existing ``AnyView`` box management, specialised value
views, mutation rules, value ABI, serialisation, Python conversion, and
time-series endpoint visitation do not change. The value visitor is also
available through the top-level ``<hgraph/hgraph.h>`` convenience header.

Initial adoption
----------------

Two existing live-value algorithms demonstrate the boundary:

* JSON tree conversion uses the visitor for atomic, indexed, and map shape
  behaviour while preserving its explicit policy for an empty ``Any``;
* the evaluation path of container length uses the visitor, while its
  wiring-time overload predicate remains a metadata switch.

Codec-plan synthesis and other metadata-only switches are deliberately not
converted.

Alternatives
------------

Expose ``AnyView`` as an alternative
   Rejected. It describes the dynamic owning box, not the semantic shape of
   its content. Requiring callers to unwrap it defeats the main dynamic-value
   use case and allows nested boxes to leak into every algorithm.

Pass atomic values as ``ValueView``
   Rejected. ``ValueView`` is also the catch-all, so a caller cannot distinguish
   an atomic handler from fallback handling. ``AtomicView`` supplies shape
   identity without closing the scalar extension set.

Enumerate known C++ scalar types
   Rejected. Extensions can register scalar types independently, and a closed
   variant would make the core SDK the owner of downstream scalar vocabulary.

Add ``match`` / ``when`` declarative wrappers
   Deferred. Callable overloads cover the required contract and match the
   endpoint visitor. A second visitor expression language is not justified.

Virtual ``accept`` or double dispatch
   Rejected. The runtime intentionally uses schemas, plans, operation tables,
   and borrowed cursors rather than a virtual value hierarchy.

Automatic recursive walking
   Rejected. Map keys, empty boxes, container order, polymorphic projection,
   depth, and cycle handling are caller policies. Recursive algorithms call
   ``visit`` again explicitly.

Acceptance criteria
-------------------

* All eight concrete semantic kinds dispatch to the correct specialised view.
* Registered extension scalar types dispatch through ``AtomicView`` without a
  core type list.
* Populated nested ``Any`` boxes dispatch transparently; empty boxes and absent
  payloads fail clearly.
* Specific handlers take precedence over ``ValueView`` catch-all handlers.
* Declared enum, fixed-list, and owned-recursive shapes are preserved.
* Read-only, writable, and mutation capability follow the selected borrowed
  value without implicit mutation.
* ``void`` and owned results compile; references, lazy ranges, incomplete
  handlers, and inconsistent result types fail at compile time.
* Existing endpoint visitor tests remain unchanged after extracting generic
  callable traits.
* JSON tree and container-length behaviour remain covered through public
  wiring APIs in C++ and Python.
* A separately built installed-SDK consumer registers and visits an extension
  scalar through ``Any``.
* Focused performance is equivalent to or better than the corresponding
  caller-written switch, with zero allocations.
* The complete native and non-WIP Python compatibility suites pass on macOS
  and Linux.
