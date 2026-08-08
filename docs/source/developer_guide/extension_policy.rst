Downstream Extension Policy
===========================

Status
------

This page records the core ownership and promotion rules applied to
independently built hgraph extensions. It also inventories extension facilities
and candidate gaps as of 24 July 2026. Candidate entries are an assessment, not
approved feature RFCs.

Ownership boundary
------------------

``hgraph`` owns the C++ runtime, type and operator systems, graph execution,
generic persistence contracts, and the public facilities required for another
library to join the same process-wide runtime safely.

A downstream extension owns its domain values, policies, algorithms, reusable
graphs, adapters, and optional Python facade. Domain-specific work should
incubate there until use demonstrates a stable, efficient, domain-independent
contract. Mathematical generality does not by itself establish core ownership.

The dependency direction is one way:

.. code-block:: text

   application -> downstream extension -> hgraph

Core hgraph code must not import, link against, or name a private downstream
package. A requirement originating in a private extension is stated in generic
terms before it enters a public core RFC.

Extension implementation rules
------------------------------

An extension follows the same C++-first model as the core:

* Native values, nodes, graph overloads, and adapters are implemented in C++.
* Pure C++ applications can use the public extension without embedding Python.
* Python exposes public native values and wiring; it does not contain a second
  implementation of runtime semantics.
* The extension links the installed shared hgraph targets and shared nanobind
  runtime. It must not statically embed another hgraph registry universe.
* Static scalar policies select overloads during wiring. A genuinely dynamic
  policy composes a graph-level switch.
* Public hot paths avoid repeated string policy checks, reflection, allocation,
  and Python calls.
* C++ and Python behaviour tests use the same cases and expected results.

Incubation and promotion
------------------------

A downstream feature is promoted only through the RFC process in
:doc:`../rfc/rfc_0000`. The proposal supplies real implementation evidence,
separates generic mechanics from domain policy, states representation and
performance goals, and proves the public surface with an installed-SDK
extension.

Promotion does not leave two authoritative implementations. Separate linked
pull requests:

#. add the accepted generic contract to ``hgraph``;
#. remove, delegate, or explicitly deprecate the downstream implementation;
#. update compatibility and serialization paths; and
#. coordinate minimum dependency versions and releases.

Existing extension facilities
-----------------------------

.. list-table::
   :header-rows: 1
   :widths: 28 18 54

   * - Facility
     - State
     - Evidence and boundary
   * - Shared native runtime and SDK
     - Available
     - Exported shared CMake targets and ``hgraph_add_python_module`` keep
       process-wide registries and the nanobind runtime unique.
   * - Native scalar/Python facade registration
     - Available
     - :doc:`../rfc/rfc_0003_extension_scalar_registration` proves a separately
       built scalar extension and bidirectional Python reflection.
   * - Typed Frame metadata
     - Available
     - :doc:`../rfc/rfc_0001_typed_frame_metadata` supplies one Arrow-backed
       ``Frame[Rows, Metadata]`` representation and schema-directed metadata.
   * - Temporal values, zones, and ranges
     - Available
     - :doc:`../rfc/rfc_0002_temporal_types` supplies native/Python values,
       range algebra, fixed-time iteration, named-zone resolution, and codecs.
   * - Python-owned structured scalar compatibility
     - Available
     - :doc:`../rfc/rfc_0004_python_owned_structured_scalars` presents declared
       Python objects through the normal Bundle contract. It is a Python
       compatibility facility, not a substitute for native extension values.
   * - Services, adapters, and record/replay composition
     - Available foundation
     - Native interfaces, graph-selected implementations, ``GlobalState``
       configuration, and component record/replay support environment-specific
       bindings without changing decision graphs.

Core RFC candidates
-------------------

The following gaps are sufficiently generic to justify focused RFC
investigation. Their ordering reflects current downstream impact.

Extension scalar codecs
~~~~~~~~~~~~~~~~~~~~~~~

**Priority: high.** A native extension scalar can register its schema and
Python facade, but the JSON and Arrow codec builders still recognize a closed
set of core atomic types. An otherwise valid extension scalar is rejected by
generic JSON, Frame metadata, table, and record/replay paths.

A proposal should define public, conflict-detecting registration of:

* schema-directed JSON encode/decode operations;
* an Arrow physical type plus scalar/array encode/decode operations;
* deterministic codec identity and versioning;
* converter-plan caching so evaluation does not perform registry lookup;
* registry-reset and extension lifetime rules; and
* an installed pure-C++ and Python extension round trip.

The core owns the registry and invocation contract. The extension owns its
wire representation, conversion functions, and migrations.

Explicit extension ABI identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Priority: high.** The installed SDK currently constrains package, Arrow, and
nanobind versions, but it does not publish an independent hgraph C++ extension
ABI identifier or perform a load-time compatibility check.

A proposal should define the compatibility identifier, which headers and
shared-library surfaces it covers, compile-time and load-time diagnostics, and
the release rules for compatible and breaking changes. It should avoid
promising a stable ABI for templates or private implementation types.

Serializable graph and binding manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Priority: medium.** Graph traits, type reflection, inspectors,
``GlobalState``, services/adapters, and record/replay configuration provide the
ingredients for environment substitution, but there is no one stable manifest
that identifies the graph contract, extension versions, selected
implementations, source/sink bindings, and reproducibility inputs for a run.

A proposal must distinguish a stable public manifest from diagnostic dumps and
must not serialize credentials or live resource handles.

Durable checkpoint and store contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Priority: medium.** The core has typed record/replay and in-memory/Arrow
foundations, but long-running applications still need a generic durable store
and checkpoint/recovery contract. Any proposal must define consistency
boundaries, schema/version migration, idempotency, and the relation between
recorded inputs, node state, and external effects.

Capabilities that remain downstream
-----------------------------------

The following may use generic mathematics but do not yet justify core
ownership:

* policy-rich streaming statistics and mergeable estimators;
* event-time, explicitly keyed, late-data, or revision-aware window policy;
* dimensional units and domain conversion catalogues;
* dynamic cross-sectional analytics;
* business calendars, sessions, settlement, and lifecycle rules; and
* domain order, position, risk, market-data, and execution models.

These capabilities should continue to gain API, dispatch, serialization, and
performance experience downstream. A later proposal applies the promotion gate
without copying domain policy into hgraph.
