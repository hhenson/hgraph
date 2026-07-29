RFC 0009: Type-Erased Time-Series Endpoint Visitors
====================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-29
:Target: Public C++ time-series endpoint views

Summary
-------

Add one-level, callable-based visitation for ``TSInputView`` and
``TSOutputView``. The visitor dispatches on the endpoint's semantic
``TSTypeKind`` and passes a shape-specific borrowed view to the selected
handler. It uses the runtime's existing type-erased schema and view surfaces;
it does not add virtual ``accept`` methods, RTTI, heap allocation, or a new
operations-table entry.

This first RFC deliberately covers endpoints only. ``ValueView``,
``TSDataView``, nodes, graphs, executors, clocks, and recursive traversal are
left for separate proposals after endpoint usage has established the right
callable conventions.

Motivation
----------

Generic endpoint algorithms currently repeat a switch on ``schema()->kind``
and manually construct ``TSSInputView``, ``TSDOutputView``, and the other
shape-specific views. The legacy C++ hgraph implementation supplied visitor
dispatch for this purpose, but its class hierarchy and double-dispatch
mechanism do not fit the current type-erased runtime.

The current runtime already carries the complete discriminator and the
layout-independent view projections required for dispatch. A free function can
therefore perform one semantic-kind switch and hand the caller the appropriate
view without introducing another object hierarchy or making the runtime aware
of caller-defined visitor classes.

Ownership boundary
------------------

Endpoint visitation is a core C++ SDK facility because generic algorithms and
independently built extensions both consume the public endpoint views. The
runtime remains the source of truth for endpoint semantics and projections.

Python receives no new API. Python already performs dynamic dispatch on its
time-series wrappers, and this C++ callable convenience does not change
runtime behaviour visible through the bridge.

Public C++ contract
-------------------

The public header ``<hgraph/types/time_series/visitor.h>`` provides:

.. code-block:: cpp

   template<class... Handlers>
   decltype(auto) visit(const TSInputView &view, Handlers&&... handlers);

   template<class... Handlers>
   decltype(auto) visit(const TSOutputView &view, Handlers&&... handlers);

Handlers are combined into one overload set. Dispatch selects the following
argument type:

.. list-table::
   :header-rows: 1
   :widths: 16 42 42

   * - Kind
     - Input handler argument
     - Output handler argument
   * - ``TS``
     - ``TSValueInputView``
     - ``TSValueOutputView``
   * - ``TSS``
     - ``TSSInputView``
     - ``TSSOutputView``
   * - ``TSD``
     - ``TSDInputView``
     - ``TSDOutputView``
   * - ``TSL``
     - ``TSLInputView``
     - ``TSLOutputView``
   * - ``TSW``
     - ``TSWInputView``
     - ``TSWOutputView``
   * - ``TSB``
     - ``TSBInputView``
     - ``TSBOutputView``
   * - ``REF``
     - ``TSReferenceInputView``
     - ``TSReferenceOutputView``
   * - ``SIGNAL``
     - ``TSSignalInputView``
     - ``TSSignalOutputView``

The six new leaf views are move-only tagged wrappers over the corresponding
erased endpoint view. They add only ``kind`` identity and the normal ``base()``
projection; collection kinds retain their existing specialised views.

A handler for the selected specialised type takes precedence. When that
handler is absent, a handler accepting the role's erased ``TSInputView`` or
``TSOutputView`` is the catch-all. A generic callable accepting the specialised
view is also valid. If neither a specialised nor base handler can accept any
reachable alternative, the visitor is ill-formed.

Every selected branch returns ``void`` or the same non-reference result type.
Reference results are rejected because the shape-specific wrapper is a
temporary borrowed cursor. The visitor forwards the returned value without
materialising an optional or allocating storage.

Endpoint and reference semantics
--------------------------------

Dispatch depends on ``view.schema()->kind``. It does not inspect the current
value schema, concrete storage implementation, or source endpoint:

* an input cursor with a schema is dispatchable even when its current value or
  peered target is invalid;
* an untyped/default endpoint throws ``std::invalid_argument``;
* a ``REF`` endpoint dispatches as ``TSReference*View`` and is not followed;
* a target link exposing a non-``REF`` schema dispatches as that exposed
  semantic kind, irrespective of the route used to reach its source; and
* collection elements and structural children are not visited automatically.

The handler receives a borrowed cursor over the same endpoint position and
evaluation time. The visitor neither owns nor extends the lifetime of the
endpoint or its underlying storage. Mutating an output through the selected
view and activating or binding an input retain the existing capability and
lifecycle rules.

Representation and performance
------------------------------

The implementation is header-only and uses one switch on ``TSTypeKind``.
Existing collection views are constructed through an internal trusted path
after that switch so their public kind validation is not repeated. Public
direct constructors remain checked.

The change adds no data member, vtable, type-record field, operation-table
slot, registry entry, or runtime dependency. The generated dispatch should be
equivalent to the existing manual switch plus view construction. A focused
benchmark compares the two forms on macOS and Linux and records any measurable
difference before acceptance.

Compatibility
-------------

The API is additive. Endpoint sizes, ownership, mutation, binding,
notification, ABI versions, serialisation, and Python behaviour do not change.
The header is installed with the existing SDK and is available through the
top-level ``<hgraph/hgraph.h>`` convenience header.

Alternatives
------------

Virtual ``accept`` and acyclic/double-dispatch visitors
   Rejected because the current runtime intentionally uses schema, plan, ops,
   and view type erasure rather than a virtual endpoint class hierarchy.

Caller-written switches
   Retained where code is dispatching metadata rather than a live endpoint,
   but repetitive endpoint-view switches should use the common visitor.

Recursive walking
   Deferred. TSD key selection, live versus modified children, reference
   following, and cycle handling are policies rather than consequences of
   single-object dispatch.

``ValueView`` visitation
   Deferred. Atomic values and open extension types require a broader
   ``std::visit``-style design and should not enlarge this endpoint RFC.

Acceptance criteria
-------------------

* All eight semantic kinds dispatch correctly for input and output endpoints.
* Specialised handlers override the role-level catch-all.
* ``void`` and common value results work without allocations; reference
  results and incomplete visitors fail at compile time.
* Invalid-current-value inputs dispatch, default views fail clearly, and
  ``REF`` is not followed.
* Existing JSON endpoint algorithms demonstrate explicit recursive use without
  semantic changes.
* A separately built installed-SDK consumer compiles and runs the visitor.
* Focused performance is equivalent to the corresponding manual switch.
* The complete native and non-WIP Python compatibility suites pass on macOS
  and Linux.

Implementation status
---------------------

The implementation accompanies this RFC in the same pull request. The RFC
status becomes ``Accepted`` when that pull request merges.
